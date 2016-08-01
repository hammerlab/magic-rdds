package org.hammerlab.magic.rdd.grid

import org.apache.spark.Partitioner

case class GridPartitioner(maxElemRow: Int,
                           maxElemCol: Int,
                           rHeight: Int,
                           cWidth: Int)
  extends Partitioner {

  val numPartitionRows = (maxElemRow + rHeight) / rHeight
  val numPartitionCols = (maxElemCol + cWidth) / cWidth

  override def numPartitions: Int =
    numPartitionRows * numPartitionCols

  override def getPartition(key: Any): Int = {
    val (r, c) = key.asInstanceOf[(Int, Int)]
    numPartitionCols * (r / rHeight) + c / cWidth
  }

  def getPartitionCoords(partitionIdx: Int): (Int, Int) =
    (partitionIdx / numPartitionCols, partitionIdx % numPartitionCols)
}

object GridPartitioner {
  def apply(maxRow: Int, maxCol: Int, partitionPenalty: Int = 100): GridPartitioner = {
    val sqrtPenalty = math.sqrt(partitionPenalty)

    /**
     * Introduce a factor of sqrt(penalty) difference between [partition width] and [num partition columns],
     * and also between [partition height] and [num partition rows].
     *
     * Overall, this will result in each partition having approximately @partitionPenalty times as many elements
     * as there are partitions.
     */
    val cWidth = math.ceil(math.sqrt(maxRow * sqrtPenalty)).toInt
    val rHeight = math.ceil(math.sqrt(maxCol * sqrtPenalty)).toInt
    GridPartitioner(maxRow, maxCol, rHeight, cWidth)
  }
}

