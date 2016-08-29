package org.hammerlab.magic.rdd.grid

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.grid.PartialSumGridRDD.{Col, Row}
import spire.algebra.Monoid

import scala.collection.mutable.{ArrayBuffer, Map => MMap}
import scala.reflect.ClassTag

/**
 * Given an [[RDD]] representing a summable value `V` at various (row, col) positions, compute a new RDD where an
 * element with key (r, c) will have its value replaced by the sum of the values of all elements with keys (R, C) where
 * R ≥ r and C ≥ c.
 *
 * @param rdd [[RDD]] with `V` values mapped to (row, col) position as on a 2-D grid.
 * @param combineValues commutative+associative function for combining `V` values.
 * @param zero `V` "zero" value.
 * @tparam V combinable value data type.
 */
class PartialSumGridRDD[V: ClassTag] private(@transient val rdd: RDD[((Row, Col), V)],
                                             combineValues: (V, V) ⇒ V,
                                             zero: V)
  extends Serializable {

  /**
   * [[rdd]] must already partitioned by a [[GridPartitioner]].
   */
  lazy val partitioner: GridPartitioner =
    rdd.partitioner match {
      case Some(gp: GridPartitioner) ⇒ gp
      case Some(p) ⇒ throw new Exception(s"Invalid partitioner: $p")
      case _ ⇒ throw new Exception(s"Missing GridPartitioner")
    }

  @transient lazy val partialSums2D: RDD[((Row, Col), V)] = {

    // Compute the row- and column-sums within each partition of [[rdd]].
    val summedPartitions: RDD[((Row, Col), V)] =
      rdd
        .mapPartitionsWithIndex(
          (idx, it) ⇒ {
            val (pRow, pCol) = partitioner.getPartitionCoords(idx)
            val rHeight = partitioner.rHeight
            val cWidth = partitioner.cWidth

            val (firstRow, lastRow) =
              (
                rHeight * pRow,
                math.min(rHeight * (pRow + 1) - 1, partitioner.maxElemRow)
              )

            val (firstCol, lastCol) =
              (
                cWidth * pCol,
                math.min(cWidth * (pCol + 1) - 1, partitioner.maxElemCol)
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
                rowSum = combineValues(curElem, rowSum)
                summedMap((r, c)) = combineValues(rowSum, elemAbove)
              }
            }
            summedMap.toIterator
          }
        )

    /**
     * Each must send [[Message]]s to partitions at lower row- and column-numbers to it, containing information about
     * some of its row- and column-sums.
     *
     * Here we create all messages and partition them by their destination partition.
     */
    val messagesRDD: RDD[Message[V]] =
      summedPartitions
        .mapPartitionsWithIndex(
          (idx, it) ⇒ {
            val (pRow, pCol) = partitioner.getPartitionCoords(idx)
            val rHeight = partitioner.rHeight
            val cWidth = partitioner.cWidth
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
        )
      .partitionBy(partitioner)
      .values

    /**
     * Combine each partition from the row- and column-summed [[RDD]] with the [[Message]]s containing information about
     * relevant sums at higher row- and column- numbers from other partitions to obtain the final [[RDD]] with each
     * position's partial-sum.
     */
    summedPartitions
      .zipPartitions(messagesRDD)(
        (iter, msgsIter) ⇒ {

          val colSums = MMap[Int, V]()
          val rowSums = MMap[Int, V]()
          var bottomLeftSum = zero

          val msgsArr = msgsIter.toArray
          val arr = iter.toArray

          msgsArr.foreach {
            case BottomLeftElem(t) ⇒
              bottomLeftSum = combineValues(bottomLeftSum, t)
            case BottomRow(m) ⇒
              for {
                (c, t) ← m
              } {
                colSums(c) = combineValues(colSums.getOrElse(c, zero), t)
              }
            case LeftCol(m) ⇒
              for {
                (r, t) ← m
              } {
                rowSums(r) = combineValues(rowSums.getOrElse(r, zero), t)
              }
          }

          for {
            ((r, c), t) ← arr.toIterator
            rowSum = rowSums.getOrElse(r, zero)
            colSum = colSums.getOrElse(c, zero)
          } yield {
            (r, c) → combineValues(colSum, combineValues(bottomLeftSum, combineValues(rowSum, t)))
          }
        }
      )
  }
}

object PartialSumGridRDD {

  type Row = Int
  type Col = Int

  /**
   * Given an [[RDD]] or grid-mapped values (with keys of the form (row, col)), compute a "partial-sum RDD", where each
   * element's value is replaced with the sum of the values of all elements with keys from higher rows and columns.
   *
   * @param rdd input [[RDD]].
   * @param m   implicit Monoid exposing "addition" and identity for type `V`.
   * @tparam V value type.
   * @return a tuple consisting of the input RDD partitioned into a grid by its key (row, col) tuples, a "partial-sum"
   *         RDD as described above, and the maximum row- and column- values found.
   */
  def apply[V: ClassTag](
    rdd: RDD[((Row, Col), V)],
    partitionDimensionsOpt: Option[(Int, Int)] = None
  )(
    implicit m: Monoid[V]
  ): (RDD[((Row, Col), V)], RDD[((Row, Col), V)], Int, Int) = {

    val (maxR, maxC) =
      rdd
        .keys
        .reduce(
          (p1, p2) ⇒
            (
              math.max(p1._1, p2._1),
              math.max(p1._2, p2._2)
            )
        )

    val partitioner =
      partitionDimensionsOpt match {
        case Some((rHeight, cWidth)) =>
          GridPartitioner(maxR, maxC, rHeight, cWidth)
        case None =>
          GridPartitioner(maxR, maxC)
      }

    val pdf = rdd.reduceByKey(partitioner, m.op(_, _))

    val partialSums = new PartialSumGridRDD[V](pdf, m.op, m.id).partialSums2D

    (
      pdf,
      partialSums,
      maxR,
      maxC
    )
  }

  def apply[V: ClassTag](
    rdd: RDD[((Row, Col), V)],
    rHeight: Int,
    cWidth: Int
  )(
    implicit m: Monoid[V]
  ): (RDD[((Row, Col), V)], RDD[((Row, Col), V)], Int, Int) =
    apply(rdd, Some((rHeight, cWidth)))

  def register(kryo: Kryo): Unit = {
    kryo.register(classOf[BottomLeftElem[_]])
    kryo.register(classOf[BottomRow[_]])
    kryo.register(classOf[LeftCol[_]])
  }
}
