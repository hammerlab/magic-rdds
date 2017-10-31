package org.hammerlab.magic.rdd.partitions

import org.apache.spark.rdd.RDD
import org.hammerlab.kryo._

import scala.reflect.ClassTag

/**
 * Helper for determining the size of each partition of an [[RDD]].
 */
trait PartitionSizes {
  implicit class PartitionSizesOps[T: ClassTag](rdd: RDD[T]) extends Serializable {
    lazy val partitionSizes =
      rdd
        .mapPartitions(
          it ⇒ Iterator(it.size),
          preservesPartitioning = true
        )
        .collect()
  }
}

object PartitionSizes
  extends spark.Registrar(
    arr[Int]
  )
