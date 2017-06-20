package org.hammerlab.magic.rdd.sliding

import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.partitions.PartitionByKeyRDD._
import org.hammerlab.magic.rdd.sliding.BorrowElemsRDD._
import org.hammerlab.magic.rdd.zip.ZipPartitionsWithIndexRDD._
import org.hammerlab.spark.PartitionIndex

import scala.reflect.ClassTag

/**
 * Wrap an [[RDD]] provide various functions for shuffling elements to adjacent partitions.
 */
case class BorrowElemsRDD[T: ClassTag](rdd: RDD[T]) {

  /**
   * Move `n` elements from each partition to the end of the partition to their left, subject to a map of overrides
   * which can be used to e.g. skip over empty partitions.
   */
  def shiftLeft(n: Int,
                partitionOverrides: PartitionOverrides = Map()): RDD[T] =
    copyN(
      n,
      (partitionIdx: Int, it: Iterator[T], tail: Iterator[T]) ⇒
        (
          if (partitionIdx == 0)
            it
          else
            it.drop(n)
        )
          ++ tail,
      partitionOverrides
    )

  def copyN(n: Int,
            fn: (Int, Iterator[T], Iterator[T]) ⇒ Iterator[T],
            partitionOverrides: PartitionOverrides = Map()): RDD[T] = {

    val N = rdd.getNumPartitions

    val partitionOverridesBroadcast = rdd.sparkContext.broadcast(partitionOverrides)

    val copiedElemsRDD: RDD[T] =
      rdd
        .mapPartitionsWithIndex((partitionIdx, iter) ⇒
          if (partitionIdx == 0)
            Iterator()
          else {
            val copiedElems = iter.take(n)

            // By default, send elements one partition to "the left".
            val sendToIdx =
              partitionOverridesBroadcast
                .value
                .getOrElse(
                  partitionIdx,
                  partitionIdx - 1
                )

            for {
              (elem, idx) ← copiedElems.zipWithIndex
            } yield
              sendToIdx → (partitionIdx, idx) → elem
          }
        )
        .partitionByKey(N)

    rdd.zipPartitionsWithIndex(copiedElemsRDD)(
      (partitionIdx, iter, tailIter) ⇒
        fn(partitionIdx, iter, tailIter)
    )
  }
}

object BorrowElemsRDD {

  type PartitionOverrides = Map[PartitionIndex, PartitionIndex]

  implicit def rddToBorrowElemsRDD[T: ClassTag](rdd: RDD[T]): BorrowElemsRDD[T] = BorrowElemsRDD(rdd)
}
