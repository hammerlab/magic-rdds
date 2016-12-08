package org.hammerlab.magic.rdd.collect

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class CollectPartitionsRDD[T: ClassTag](rdd: RDD[T]) extends Serializable {
  def collectParts: Array[Array[T]] =
    rdd.sparkContext.runJob(rdd, (iter: Iterator[T]) => iter.toArray)
}

object CollectPartitionsRDD {
  implicit def toCollectPartitionsRDD[T: ClassTag](rdd: RDD[T]): CollectPartitionsRDD[T] =
    new CollectPartitionsRDD(rdd)
}
