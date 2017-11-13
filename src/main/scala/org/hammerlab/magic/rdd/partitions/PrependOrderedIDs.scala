package org.hammerlab.magic.rdd.partitions

import org.apache.spark.rdd.RDD
import org.hammerlab.spark.PartitionIndex

import scala.reflect.ClassTag

/**
 * Lazily key each element by its partition number and intra-partition idx.
 *
 * Useful in tandem with [[PartitionByKey]].
 */
trait PrependOrderedIDs {
  implicit class PrependOrderedIDsOps[T: ClassTag](rdd: RDD[T]) {
    def prependOrderedIDs: RDD[((PartitionIndex, Int), T)] =
      rdd.
        mapPartitionsWithIndex(
          (partitionIdx, it) ⇒
            for {
              (elem, idx) ← it.zipWithIndex
            } yield
              partitionIdx → idx → elem
        )
  }
}
