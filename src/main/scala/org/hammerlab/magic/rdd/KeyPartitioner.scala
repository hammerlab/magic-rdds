package org.hammerlab.magic.rdd

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

case class KeyPartitioner(numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = key match {
    case i: Int            => i
    case (idx: Int, _)     => idx
    case other             => throw new AssertionError(s"Unexpected key: $other")
  }
}

object KeyPartitioner {
  def apply(rdd: RDD[_]): KeyPartitioner = KeyPartitioner(rdd.getNumPartitions)
}
