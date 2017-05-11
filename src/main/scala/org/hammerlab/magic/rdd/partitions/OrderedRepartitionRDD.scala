package org.hammerlab.magic.rdd.partitions

import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.partitions.OrderedRepartitionRDD._

import scala.reflect.ClassTag

/**
 * Some helpers for repartitioning an [[RDD]] while retaining the order of its elements.
 */
case class OrderedRepartitionRDD[T: ClassTag](@transient rdd: RDD[T]) {
  def orderedRepartition(n: Int): RDD[T] =
    rdd
      .zipRepartition(n)
      .values

  def zipRepartition(n: Int): RDD[(Long, T)] =
    rdd
      .zipWithIndex
      .map(_.swap)
      .sortByKey(numPartitions = n)
}

object OrderedRepartitionRDD {
  implicit def toOrderedRepartitionRDD[T: ClassTag](rdd: RDD[T]): OrderedRepartitionRDD[T] = OrderedRepartitionRDD(rdd)
}

