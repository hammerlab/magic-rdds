package org.hammerlab.magic.rdd.partitions

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Helper for determining the size of each partition of an [[RDD]].
 */
class PartitionSizesRDD[T: ClassTag](rdd: RDD[T]) {
  lazy val partitionSizes =
    rdd.mapPartitionsWithIndex((idx, it) ⇒ {
      Iterator(it.size)
    }).collect()
}

object PartitionSizesRDD {
  implicit def rddToPartitionSizesRDD[T: ClassTag](rdd: RDD[T]): PartitionSizesRDD[T] = new PartitionSizesRDD(rdd)
}
