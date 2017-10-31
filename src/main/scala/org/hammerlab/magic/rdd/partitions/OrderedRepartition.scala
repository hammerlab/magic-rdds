package org.hammerlab.magic.rdd.partitions

import org.apache.spark.rdd.RDD
import org.hammerlab.spark.PartitionIndex

import scala.reflect.ClassTag

/**
 * Some helpers for repartitioning an [[RDD]] while retaining the order of its elements.
 */
trait OrderedRepartition {
  implicit class OrderedRepartitionOps[T: ClassTag](rdd: RDD[T]) extends Serializable {
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
}

