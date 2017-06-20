package org.hammerlab.magic.rdd.partitions

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Helper APIs for reducing [[RDD]] partitions to single elements, either with a combiner function
 * ([[reducePartitions]]) or by directly mapping a partition [[Iterator]] to a single element ([[collapsePartitions]]).
 */
case class ReducePartitionsRDD[T](rdd: RDD[T]) {
  def reducePartitionsRDD[U: ClassTag](init: U)(fn: (U, T) ⇒ U): RDD[U] =
    rdd
      .mapPartitions(
        it ⇒
          Iterator(
            it.foldLeft(init)(fn)
          )
      )

  def reducePartitions[U: ClassTag](init: U)(fn: (U, T) ⇒ U): Array[U] =
    reducePartitionsRDD(init)(fn).collect()

  def collapsePartitionsRDD[U: ClassTag](fn: Iterator[T] ⇒ U): RDD[U] =
    rdd.mapPartitions(it ⇒ Iterator(fn(it)))

  def collapsePartitions[U: ClassTag](fn: Iterator[T] ⇒ U): Array[U] =
    collapsePartitionsRDD(fn).collect()

}

object ReducePartitionsRDD {
  implicit def makeReducePartitionsRDD[T](rdd: RDD[T]): ReducePartitionsRDD[T] = ReducePartitionsRDD(rdd)
}
