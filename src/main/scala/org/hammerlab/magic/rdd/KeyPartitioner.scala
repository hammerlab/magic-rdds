package org.hammerlab.magic.rdd

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

/**
 * Spark [[Partitioner]] that maps elements to a partition indicated by an [[Int]] that either is the key, or is the
 * first element of a tuple.
 */
case class KeyPartitioner(numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int =
    key match {
      case i: Int                 => i
      case Product2(k: Int, _)    => k
      case Product3(k: Int, _, _) => k
      case other             => throw new AssertionError(s"Unexpected key: $other")
    }
}

object KeyPartitioner {
  def apply(rdd: RDD[_]): KeyPartitioner = KeyPartitioner(rdd.getNumPartitions)
}
