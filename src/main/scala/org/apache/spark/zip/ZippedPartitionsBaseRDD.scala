package org.apache.spark.zip

import org.apache.spark.{Partition, SparkContext}
import org.apache.spark.rdd.{RDD, ZippedPartitionsBaseRDD => SparkZippedPartitionsBaseRDD, ZippedPartitionsPartition => SparkZippedPartitionsPartition}

import scala.reflect.ClassTag

/**
 * Package-cheat to expose [[org.apache.spark.rdd.ZippedPartitionsBaseRDD]].
 */
abstract class ZippedPartitionsBaseRDD[V: ClassTag](sc: SparkContext,
                                                    rddsBase: Seq[RDD[_]],
                                                    preservesPartitioning: Boolean = false)
  extends SparkZippedPartitionsBaseRDD[V](sc, rddsBase, preservesPartitioning) {

  // Replace Spark's ZippedPartitionsPartitions with ours.
  override def getPartitions: Array[Partition] =
    for {
      (partition, idx) <- super.getPartitions.zipWithIndex
    } yield
      new ZippedPartitionsPartition(
        idx,
        rddsBase,
        partition.asInstanceOf[SparkZippedPartitionsPartition].preferredLocations
      )
}
