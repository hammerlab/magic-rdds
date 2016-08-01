package org.hammerlab.magic.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Some helpers for repartitioning an [[RDD]] while retaining the order of its elements.
 */
class OrderedRepartitionRDD[T: ClassTag](@transient val rdd: RDD[T]) extends Serializable {
  def orderedRepartition(n: Int): RDD[T] =
    rdd
      .zipWithIndex
      .map(_.swap)
      .sortByKey(numPartitions = n)
      .values

  def zipRepartition(n: Int): RDD[(Long, T)] =
    rdd
      .zipWithIndex.map(_.swap)
      .sortByKey(numPartitions = n)

  def zipRepartitionL(n: Int): RDD[(T, Long)] =
    rdd
      .zipWithIndex.map(_.swap)
      .sortByKey(numPartitions = n)
      .map(_.swap)
}

object OrderedRepartitionRDD {
  implicit def toOrderedRepartitionRDD[T: ClassTag](rdd: RDD[T]): OrderedRepartitionRDD[T] = new OrderedRepartitionRDD(rdd)
}

