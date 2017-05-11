package org.hammerlab.magic.rdd.partitions

import org.apache.spark.rdd.RDD
import org.hammerlab.spark.PartitionIndex

import scala.collection.SortedSet

case class FilterPartitionIdxs[T](@transient rdd: RDD[T]) {
  def filterPartitionIdxs(fn: Iterator[T] ⇒ Boolean): SortedSet[PartitionIndex] =
    SortedSet(
      rdd
        .mapPartitionsWithIndex(
          (idx, it) ⇒
            if (fn(it))
              Iterator(idx)
            else
              Iterator()
        )
        .collect(): _*
    )

}

object FilterPartitionIdxs {
  implicit def makeFilterPartitionIdxs[T](rdd: RDD[T]): FilterPartitionIdxs[T] = FilterPartitionIdxs(rdd)
}
