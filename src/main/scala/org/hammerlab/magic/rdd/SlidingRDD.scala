package org.hammerlab.magic.rdd

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.hammerlab.magic.iterator.TakeUntilIterator
import org.hammerlab.magic.rdd.BorrowElemsRDD._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class SlidingRDD[T: ClassTag](@transient rdd: RDD[T]) extends Serializable {
  def sliding2(fill: T): RDD[(T, T)] = sliding2(Some(fill))
  def sliding2(fillOpt: Option[T] = None): RDD[(T, T)] =
    rdd
      .copy(1, fillOpt)
      .mapPartitions(
        _
          .sliding(2)
          .withPartial(false)
          .map(l => (l(0), l(1)))
      )

  def sliding3(fill: T): RDD[(T, T, T)] = sliding3(Some(fill))
  def sliding3(fillOpt: Option[T] = None): RDD[(T, T, T)] =
    rdd
      .copy(2, fillOpt)
      .mapPartitions(
        _
          .sliding(3)
          .withPartial(false)
          .map(l => (l(0), l(1), l(2)))
      )


  def sliding(n: Int, fill: T): RDD[Seq[T]] = sliding(n, Some(fill))
  def sliding(n: Int, fillOpt: Option[T] = None): RDD[Seq[T]] =
    rdd
      .copy(n - 1, fillOpt)
      .mapPartitions(
        _
          .sliding(n)
          .withPartial(false)
      )

  def slideUntil(sentinel: T): RDD[Seq[T]] = {
    val N = rdd.getNumPartitions
    val firstSplit: RDD[(Int, ArrayBuffer[T])] =
      rdd.mapPartitionsWithIndex((idx, iter) =>
          if (idx == 0)
            Iterator()
          else {
            val toSentinel: ArrayBuffer[T] = ArrayBuffer()
            val buffered = iter.buffered
            while (buffered.hasNext && buffered.head != sentinel) {
              toSentinel.append(buffered.next())
            }
            if (!buffered.hasNext) {
              throw new IllegalArgumentException(s"Partition $idx did not include sentinel $sentinel:\n${toSentinel.take(100).mkString(",")}")
            }
            Iterator(
              (idx - 1) → toSentinel
            )
          }
      ).partitionBy(new KeyPartitioner(N))

    rdd.zipPartitions(firstSplit)((iter, tailIter) ⇒ {
      val (partitionIdx, tail) =
        if (tailIter.hasNext) {
          val (pIdx, buf) = tailIter.next()
          (pIdx, buf.toIterator)
        } else {
          (N - 1, Iterator())
        }

      val it =
        (if (partitionIdx == 0)
          iter
        else
          iter.dropWhile(_ != sentinel)
        ) ++ tail

      new TakeUntilIterator[T](it, sentinel)
    })
  }

}

object SlidingRDD {
  implicit def toSlidingRDD[T: ClassTag](rdd: RDD[T]): SlidingRDD[T] = new SlidingRDD[T](rdd)
}
