package org.apache.spark.zip

import org.apache.spark.Partition
import org.apache.spark.rdd.{ZippedWithIndexRDDPartition => SparkZippedWithIndexRDDPartition}

/**
 * Package-cheat to expose [[org.apache.spark.rdd.ZippedWithIndexRDDPartition]].
 */
class ZippedWithIndexRDDPartition(override val prev: Partition,
                                  override val startIndex: Long)
  extends SparkZippedWithIndexRDDPartition(prev, startIndex)
