package org.hammerlab.magic.rdd

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ArrayBuffer, Map => MMap}
import scala.reflect.ClassTag

case class GridPartitioner(maxRow: Int, maxCol: Int, rHeight: Int, cWidth: Int) extends Partitioner {
  val partitionRows = (maxRow + rHeight - 1) / rHeight
  val partitionCols = (maxCol + cWidth - 1) / cWidth

  override def numPartitions: Int = {
    partitionRows * partitionCols
  }

  override def getPartition(key: Any): Int = {
    val (r, c) = key.asInstanceOf[(Int, Int)]
    partitionCols * (r / rHeight) + c / cWidth
  }

  def getPartitionCoords(partitionIdx: Int): (Int, Int) = {
    (partitionIdx / partitionCols, partitionIdx % partitionCols)
  }
}

object GridPartitioner {
  def apply(maxRow: Int, maxCol: Int, partitionPenalty: Int = 100): GridPartitioner = {
    val sqrtPenalty = math.sqrt(partitionPenalty)

    // Introduce a factor of sqrt(penalty) difference between [partition width] and [num partition columns],
    // and also between [partition height] and [num partition rows].
    //
    // Overall, this will result in each partition having approximately @partitionPenalty times as many elements
    // as there are partitions.
    val cWidth = math.ceil(math.sqrt(maxRow * sqrtPenalty)).toInt
    val rHeight = math.ceil(math.sqrt(maxCol * sqrtPenalty)).toInt
    GridPartitioner(maxRow, maxCol, rHeight, cWidth)
  }
}

sealed trait Message[T]
case class BottomLeftElem[T](t: T) extends Message[T]
case class LeftCol[T](m: Map[Int, T]) extends Message[T]
case class BottomRow[T](m: Map[Int, T]) extends Message[T]

class GridCDFRDD[T: ClassTag](@transient val rdd: RDD[((Int, Int), T)]) extends Serializable {
  val partitioner = rdd.partitioner match {
    case Some(gp: GridPartitioner) ⇒ gp
    case Some(p) ⇒ throw new Exception(s"Invalid partitioner: $p")
    case _ ⇒ throw new Exception(s"Missing GridPartitioner")
  }

  def cdf(fn: (T, T) ⇒ T, zero: T): RDD[((Int, Int), T)] = {

    val summedPartitions =
      rdd.mapPartitionsWithIndex((idx, it) ⇒ {
        val (pRow, pCol) = partitioner.getPartitionCoords(idx)
        val rHeight = partitioner.rHeight
        val cWidth = partitioner.cWidth

        val (firstRow, lastRow) = (rHeight * pRow, math.min(rHeight * (pRow + 1) - 1, partitioner.maxRow - 1))
        val (firstCol, lastCol) = (cWidth * pCol, math.min(cWidth * (pCol + 1) - 1, partitioner.maxCol - 1))

        val map = it.toMap
        val summedMap = MMap[(Int, Int), T]()
        for {
          r ← lastRow to firstRow by -1
        } {
          var rowSum = zero
          for {
            c ← lastCol to firstCol by -1
            curElem = map.getOrElse((r, c), zero)
            elemAbove = summedMap.getOrElse((r + 1, c), zero)
          } {
            rowSum = fn(curElem, rowSum)
            summedMap((r, c)) = fn(rowSum, elemAbove)
          }
        }
        summedMap.toIterator
      })

    val messagesRDD: RDD[Message[T]] =
      summedPartitions.mapPartitionsWithIndex((idx, it) ⇒ {
        val (pRow, pCol) = partitioner.getPartitionCoords(idx)
        val rHeight = partitioner.rHeight
        val cWidth = partitioner.cWidth
        val firstRow = rHeight * pRow
        val firstCol = cWidth * pCol

        val leftColBuf = ArrayBuffer[(Int, T)]()
        val bottomRowBuf = ArrayBuffer[(Int, T)]()
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

        val messages = ArrayBuffer[((Int, Int), Message[T])]()

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
      }).partitionBy(partitioner).values

    summedPartitions.zipPartitions(messagesRDD)((iter, msgsIter) ⇒ {
      val colSums = MMap[Int, T]()
      val rowSums = MMap[Int, T]()
      var bottomLeftSum = zero

      val msgsArr = msgsIter.toArray
      val arr = iter.toArray

      msgsArr.foreach {
        case BottomLeftElem(t) ⇒
          bottomLeftSum = fn(bottomLeftSum, t)
        case BottomRow(m) ⇒
          for {
            (c, t) ← m
          } {
            colSums(c) = fn(colSums.getOrElse(c, zero), t)
          }
        case LeftCol(m) ⇒
          for {
            (r, t) ← m
          } {
            rowSums(r) = fn(rowSums.getOrElse(r, zero), t)
          }
      }

      val pRow = arr.head._1._1 / partitioner.rHeight
      val pCol = arr.head._1._2 / partitioner.cWidth

      for {
        ((r, c), t) ← arr.toIterator
        rowSum = rowSums.getOrElse(r, zero)
        colSum = colSums.getOrElse(c, zero)
      } yield {
        (r, c) → fn(colSum, fn(bottomLeftSum, fn(rowSum, t)))
      }
    })
  }
}

object GridCDFRDD {
  implicit def rddToGridCDFRDD[T: ClassTag](rdd: RDD[((Int, Int), T)]): GridCDFRDD[T] = {
    val (maxR, maxC) = rdd.keys.reduce((p1, p2) ⇒ (math.max(p1._1, p2._1), math.max(p1._2, p2._2)))
    val partitioner = GridPartitioner(maxR + 1, maxC + 1)
    new GridCDFRDD(rdd.partitionBy(partitioner))
  }

  def rddToGridCDFRDD[T: ClassTag](rdd: RDD[((Int, Int), T)], rHeight: Int, cWidth: Int): GridCDFRDD[T] = {
    val (maxR, maxC) = rdd.keys.reduce((p1, p2) ⇒ (math.max(p1._1, p2._1), math.max(p1._2, p2._2)))
    val partitioner = GridPartitioner(maxR + 1, maxC + 1, rHeight, cWidth)
    new GridCDFRDD(rdd.partitionBy(partitioner))
  }

  def apply[T: ClassTag, U: ClassTag](rdd: RDD[U],
                                      rowFn: U ⇒ Int,
                                      colFn: U ⇒ Int,
                                      tFn: U ⇒ T,
                                      fn: (T, T) ⇒ T,
                                      zero: T): (RDD[((Int, Int), T)], RDD[((Int, Int), T)], Int, Int) = {
    val ts = rdd.map(u ⇒ (rowFn(u), colFn(u)) → tFn(u))
    val (maxR, maxC) = ts.keys.reduce((p1, p2) ⇒ (math.max(p1._1, p2._1), math.max(p1._2, p2._2)))
    val partitioner = GridPartitioner(maxR, maxC)
    val pdf = ts.reduceByKey(partitioner, fn)
    (pdf, pdf.cdf(fn, zero), maxR, maxC)
  }
}
