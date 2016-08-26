package org.apache.spark.zip

import org.apache.spark.rdd.{RDD, ZippedPartitionsPartition => SparkZippedPartitionsPartition}

/**
 * Package-cheat to expose [[org.apache.spark.rdd.ZippedPartitionsPartition]].
 */
class ZippedPartitionsPartition(idx: Int,
                                @transient private val rdds: Seq[RDD[_]],
                                @transient override val preferredLocations: Seq[String])
  extends SparkZippedPartitionsPartition(idx, rdds, preferredLocations)
