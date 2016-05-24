package org.hammerlab.magic.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class IfRDD[T: ClassTag](@transient val rdd: RDD[T]) extends Serializable {
  def iff(b: Boolean, ifFn: (RDD[T]) => RDD[T]): RDD[T] =
    if (b)
      ifFn(rdd)
    else
      rdd
}

object IfRDD {
  implicit def toIfRDD[T: ClassTag](rdd: RDD[T]): IfRDD[T] = new IfRDD(rdd)
}
