package org.hammerlab.magic.rdd.grid

import cats.Monoid
import hammerlab.monoid._
import magic_rdds._
import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.grid.PrefixSum.{ Col, GridRDD, Row }

import scala.collection.mutable.{ ArrayBuffer, Map ⇒ MMap }
import scala.math.max
import scala.reflect.ClassTag

object Result {

  /**
   * Given an [[RDD]] or grid-mapped values (with keys of the form (row, col)), compute a "partial-sum RDD", where each
   * element's value is replaced with the sum of the values of all elements with keys from higher (or equal) rows and
   * columns.
   *
   * @param rdd input [[RDD]].
   * @tparam V value type.
   * @return a tuple consisting of the input RDD partitioned into a grid by its key (row, col) tuples, a "partial-sum"
   *         RDD as described above, and the maximum row- and column- values found.
   */
  def apply[V: ClassTag: Monoid](
    rdd: GridRDD[V],
    partitionDimensionsOpt: Option[(Int, Int)] = None
  ): Result[V] = {

    val (maxR, maxC) =
      rdd
        .keys
        .reduce {
          case ((r1, c1), (r2, c2)) ⇒
            (
              max(r1, r2),
              max(c1, c2)
            )
        }

    implicit val partitioner =
      partitionDimensionsOpt match {
        case Some((rHeight, cWidth)) ⇒
          GridPartitioner(maxR, maxC, rHeight, cWidth)
        case None ⇒
          GridPartitioner(maxR, maxC)
      }

    val pdf = rdd.sumByKey(partitioner)

    val cdf = partialSums(pdf)

    Result(
      pdf,
      cdf,
      maxR,
      maxC
    )
  }

  def apply[V: ClassTag : Monoid](
    rdd: GridRDD[V],
    rHeight: Int,
    cWidth: Int
  ): Result[V] =
    apply(
      rdd,
      Some((rHeight, cWidth))
    )

  /**
   * Given an [[RDD]] representing a summable value `V` at various (row, col) positions, compute a new RDD where an
   * element with key (r, c) will have its value replaced by the sum of the values of all elements with keys (R, C) where
   * R ≥ r and C ≥ c.
   *
   * @param rdd [[RDD]] with `V` values mapped to (row, col) position as on a 2-D grid.
   * @tparam V combinable value data type.
   */
  def partialSums[V](rdd: GridRDD[V])(implicit
                                      m: Monoid[V],
                                      partitioner: GridPartitioner): GridRDD[V] = {
    val zero = m.empty

    val GridPartitioner(maxRow, maxCol, rHeight, cWidth) = partitioner

    // Compute the row- and column-sums within each partition of [[rdd]].
    val summedPartitions: GridRDD[V] =
      rdd
        .mapPartitionsWithIndex {
          (idx, it) ⇒
            val (pRow, pCol) = partitioner.getCoords(idx)

            val (firstRow, lastRow) =
              (
                rHeight * pRow,
                math.min(rHeight * (pRow + 1) - 1, maxRow)
              )

            val (firstCol, lastCol) =
              (
                cWidth * pCol,
                math.min(cWidth * (pCol + 1) - 1, maxCol)
              )

            val map = it.toMap
            val summedMap = MMap[(Row, Col), V]()
            for {
              r ← lastRow to firstRow by -1
            } {
              var rowSum = zero
              for {
                c ← lastCol to firstCol by -1
                curElem = map.getOrElse((r, c), zero)
                elemAbove = summedMap.getOrElse((r + 1, c), zero)
              } {
                rowSum = curElem |+| rowSum
                summedMap((r, c)) = rowSum |+| elemAbove
              }
            }
            summedMap.toIterator
        }


    /**
     * Each must send [[Message]]s to partitions at lower row- and column-numbers to it, containing information about
     * some of its row- and column-sums.
     *
     * Here we create all messages and partition them by their destination partition.
     */
    val messagesRDD: RDD[Message[V]] =
      summedPartitions
        .mapPartitionsWithIndex {
          (idx, it) ⇒
            val (pRow, pCol) = partitioner.getCoords(idx)
            val firstRow = rHeight * pRow
            val firstCol = cWidth * pCol

            val leftColBuf = ArrayBuffer[(Int, V)]()
            val bottomRowBuf = ArrayBuffer[(Int, V)]()
            var bottomLeft = zero
            for {
              ((r, c), t) ← it
            } {
              if (r == firstRow) {
                bottomRowBuf.append((c, t))
                if (c == firstCol) {
                  bottomLeft = t
                }
              }
              if (c == firstCol) {
                leftColBuf.append((r, t))
              }
            }

            val bottomRow = BottomRow(bottomRowBuf.toMap)
            val leftCol = LeftCol(leftColBuf.toMap)

            val messages = ArrayBuffer[((Row, Col), Message[V])]()

            for {
              destPRow ← 0 until pRow
              destPCol ← 0 until pCol
            } {
              messages.append((destPRow * rHeight, destPCol * cWidth) → BottomLeftElem(bottomLeft))
            }

            for {
              destPCol ← 0 until pCol
            } yield {
              messages.append((pRow * rHeight, destPCol * cWidth) → leftCol)
            }

            for {
              destPRow ← 0 until pRow
            } yield {
              messages.append((destPRow * rHeight, pCol * cWidth) → bottomRow)
            }

            messages.toIterator
        }
        .partitionBy(partitioner)
        .values

    /**
     * Combine each partition from the row- and column-summed [[RDD]] with the [[Message]]s containing information about
     * relevant sums at higher row- and column- numbers from other partitions to obtain the final [[RDD]] with each
     * position's partial-sum.
     */
    summedPartitions
      .zipPartitions(messagesRDD) {
        (iter, msgsIter) ⇒

          val colSums = MMap[Int, V]().withDefaultValue(zero)
          val rowSums = MMap[Int, V]().withDefaultValue(zero)
          var bottomLeftSum = zero

          msgsIter.foreach {
            case BottomLeftElem(t) ⇒
              bottomLeftSum = bottomLeftSum |+| t
            case BottomRow(r) ⇒
              for {
                (c, t) ← r
              } {
                colSums(c) = colSums(c) |+| t
              }
            case LeftCol(c) ⇒
              for {
                (r, t) ← c
              } {
                rowSums(r) = rowSums(r) |+| t
              }
          }

          for {
            ((r, c), t) ← iter
            rowSum = rowSums(r)
            colSum = colSums(c)
          } yield {
            (r, c) → (colSum |+| bottomLeftSum |+| rowSum |+| t)
          }
      }
  }
}

case class Result[V](pdf: GridRDD[V],
                     cdf: GridRDD[V],
                     maxRow: Row,
                     maxCol: Col)
