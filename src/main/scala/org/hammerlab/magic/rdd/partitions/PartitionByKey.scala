package org.hammerlab.magic.rdd.partitions

import org.apache.spark.rdd.RDD
import org.hammerlab.spark.{ KeyPartitioner, PartitionIndex }

import scala.reflect.ClassTag

/**
 * Add `partitionByKey` method to paired [[RDD]]s whose key is a tuple of (partition idx, elem idx), which sends
 * elements to the partition indicated by `partition idx`, and sorts them within each partition according to `elem idx`.
 */
trait PartitionByKey {
  implicit class PartitionByKeyOps[K: Ordering, V: ClassTag](rdd: RDD[((PartitionIndex, K), V)]) extends Serializable {
    def partitionByKey(other: RDD[_]): RDD[V] = partitionByKey(other.getNumPartitions)
    def partitionByKey(numPartitions: Int): RDD[V] =
      rdd
        .repartitionAndSortWithinPartitions(KeyPartitioner(numPartitions))
        .values
  }
}
