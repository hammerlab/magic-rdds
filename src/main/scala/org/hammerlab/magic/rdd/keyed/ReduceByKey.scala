package org.hammerlab.magic.rdd.keyed

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait ReduceByKey {
  /**
   * Adds [[maxByKey]] and [[minByKey]] helpers to an [[RDD]].
   */
  implicit class ReduceByKeyOps[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)])(implicit ord: Ordering[V]) extends Serializable {
    def maxByKey(numPartitions: Int = rdd.getNumPartitions): RDD[(K, V)] = rdd.reduceByKey((a, b) ⇒ ord.max(a, b))
    def minByKey(numPartitions: Int = rdd.getNumPartitions): RDD[(K, V)] = rdd.reduceByKey((a, b) ⇒ ord.min(a, b))
  }
}
