package org.hammerlab.magic.rdd.partitions

import org.apache.spark.rdd.RDD
import org.hammerlab.spark.PartitionIndex

import scala.collection.SortedSet

trait FilterPartitionIdxs {
  implicit class FilterPartitionIdxsOps[T](rdd: RDD[T])
    extends Serializable {
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
}
