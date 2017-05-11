package org.hammerlab.magic.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Hang an `iff` method off of [[RDD]]s, as a small bit of syntactic sugar.
 */
case class IfRDD[T: ClassTag](@transient rdd: RDD[T]) {
  def iff(b: Boolean, ifFn: (RDD[T]) ⇒ RDD[T]): RDD[T] =
    if (b)
      ifFn(rdd)
    else
      rdd
}

object IfRDD {
  implicit def makeIfRDD[T: ClassTag](rdd: RDD[T]): IfRDD[T] = IfRDD(rdd)
}
