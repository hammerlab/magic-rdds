package org.hammerlab.magic.rdd.keyed

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Adds [[maxByKey]] and [[minByKey]] helpers to an [[RDD]].
 */
case class ReduceByKeyRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)])(implicit ord: Ordering[V]) {
  def maxByKey(numPartitions: Int = rdd.getNumPartitions): RDD[(K, V)] = rdd.reduceByKey((a, b) ⇒ ord.max(a, b))
  def minByKey(numPartitions: Int = rdd.getNumPartitions): RDD[(K, V)] = rdd.reduceByKey((a, b) ⇒ ord.min(a, b))
}

object ReduceByKeyRDD {
  implicit def rddToReduceByKeyRDD[K: ClassTag, V: ClassTag : Ordering](rdd: RDD[(K, V)]): ReduceByKeyRDD[K, V] =
    ReduceByKeyRDD(rdd)
}
