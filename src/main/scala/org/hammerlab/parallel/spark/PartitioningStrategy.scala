package org.hammerlab.parallel.spark

import org.hammerlab.math.ceil

/**
 * Configuration for how to parallelize with Spark using a fixed number of partitions [[NumPartitions]] or
 * partition-size [[ElemsPerPartition]].
 */
trait PartitioningStrategy {
  def numPartitions(numElems: Int): Int
}

object PartitioningStrategy {
  implicit val default = ElemsPerPartition(1)
}

case class NumPartitions(n: Int)
  extends PartitioningStrategy {
  override def numPartitions(numElems: Int): Int = n
}

case class ElemsPerPartition(n: Int)
  extends PartitioningStrategy {
  override def numPartitions(numElems: Int): Int = ceil(numElems, n)
}
