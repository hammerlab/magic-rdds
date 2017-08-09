package org.hammerlab.magic.rdd.partitions

import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.partitions.OrderedRepartitionRDD._
import org.hammerlab.spark.PartitionIndex

import scala.reflect.ClassTag

/**
 * Some helpers for repartitioning an [[RDD]] while retaining the order of its elements.
 */
case class OrderedRepartitionRDD[T: ClassTag](@transient rdd: RDD[T]) {
  def orderedRepartition(n: Int): RDD[T] =
    zipRepartition(n)
      .values

  def zipRepartition(n: Int): RDD[((PartitionIndex, Int), T)] =
    rdd
      .mapPartitionsWithIndex {
        case (partitionIdx, elems) ⇒
          elems
            .zipWithIndex
            .map {
              case (elem, idx) ⇒
                partitionIdx → idx → elem
            }
      }
      .sortByKey(numPartitions = n)
}

object OrderedRepartitionRDD {
  implicit def toOrderedRepartitionRDD[T: ClassTag](rdd: RDD[T]): OrderedRepartitionRDD[T] = OrderedRepartitionRDD(rdd)
}

