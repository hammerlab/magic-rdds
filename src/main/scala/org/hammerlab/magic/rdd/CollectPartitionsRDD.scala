package org.hammerlab.magic.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class CollectPartitionsRDD[T: ClassTag](@transient rdd: RDD[T]) extends Serializable {
  def collectPartitions(): Array[Array[T]] = rdd.mapPartitions(it => Iterator(it.toArray)).collect()
}

object CollectPartitionsRDD {
  implicit def rddToCollectPartitionsRDD[T: ClassTag](rdd: RDD[T]): CollectPartitionsRDD[T] = new CollectPartitionsRDD(rdd)
}
