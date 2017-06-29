package org.hammerlab.spark

import org.apache.spark

case class Partitioner[T](numPartitions: NumPartitions,
                          fn: T ⇒ PartitionIndex)
  extends spark.Partitioner {
  override def getPartition(key: Any): PartitionIndex =
    fn(key.asInstanceOf[T])
}
