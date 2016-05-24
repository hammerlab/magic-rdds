package org.hammerlab.magic.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class ReduceByKeyRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)])(implicit ord: Ordering[V]) {
  def maxByKey(numPartitions: Int = rdd.getNumPartitions): RDD[(K, V)] = rdd.reduceByKey((a, b) => ord.max(a, b))
  def minByKey(numPartitions: Int = rdd.getNumPartitions): RDD[(K, V)] = rdd.reduceByKey((a, b) => ord.min(a, b))
}

object ReduceByKeyRDD {
  implicit def rddToReduceByKeyRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)])(implicit ord: Ordering[V]): ReduceByKeyRDD[K, V] =
    new ReduceByKeyRDD(rdd)
}
