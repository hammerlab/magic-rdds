package org.hammerlab.magic.rdd.grid

import org.apache.spark.Partitioner
import org.hammerlab.magic.rdd.grid.PrefixSum.{ Col, Row }
import org.hammerlab.spark.{ NumPartitions, PartitionIndex }

/**
 * Partitioner that breaks a [[org.hammerlab.magic.rdd.grid.PrefixSum.GridRDD GridRDD]] into rectangles of fixed width
 * and height
 */
case class GridPartitioner(maxRow: Row,
                           maxCol: Col,
                           rHeight: Row,
                           cWidth: Col)
  extends Partitioner {

  val numPartitionRows = (maxRow + rHeight) / rHeight
  val numPartitionCols = (maxCol + cWidth) / cWidth

  override def numPartitions: NumPartitions =
    numPartitionRows * numPartitionCols

  override def getPartition(key: Any): PartitionIndex = {
    val (r, c) = key.asInstanceOf[(Row, Col)]
    numPartitionCols * (r / rHeight) + c / cWidth
  }

  def getCoords(partitionIdx: PartitionIndex): (Row, Col) =
    (partitionIdx / numPartitionCols, partitionIdx % numPartitionCols)
}

object GridPartitioner {
  def apply(maxRow: Row, maxCol: Col, partitionPenalty: Int = 100): GridPartitioner = {
    val sqrtPenalty = math.sqrt(partitionPenalty)

    /**
     * Introduce a factor of sqrt(penalty) difference between [partition width] and [num partition columns],
     * and also between [partition height] and [num partition rows].
     *
     * Overall, this will result in each partition having approximately `partitionPenalty` times as many elements
     * as there are partitions.
     */
    val cWidth = math.ceil(math.sqrt(maxRow * sqrtPenalty)).toInt
    val rHeight = math.ceil(math.sqrt(maxCol * sqrtPenalty)).toInt
    GridPartitioner(maxRow, maxCol, rHeight, cWidth)
  }
}

