package org.hammerlab.magic.rdd.keyed

import cats.Monoid
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.hammerlab.spark.NumPartitions

import scala.reflect.ClassTag

trait ReduceByKey {
  /**
   * Adds [[maxByKey]] and [[minByKey]] helpers to an [[RDD]].
   */
  implicit class ReduceByKeyOps[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)])(implicit ord: Ordering[V]) extends Serializable {
    def maxByKey(numPartitions: NumPartitions = rdd.getNumPartitions): RDD[(K, V)] = rdd.reduceByKey(ord.max _, numPartitions)
    def minByKey(numPartitions: NumPartitions = rdd.getNumPartitions): RDD[(K, V)] = rdd.reduceByKey(ord.min _, numPartitions)
  }

  implicit class MonoidByKeyOps[K: ClassTag, V: ClassTag](val rdd: RDD[(K, V)]) extends Serializable {
    def sumByKey(implicit m: Monoid[V]): RDD[(K, V)] = rdd.reduceByKey(m.combine _)
    def sumByKey(partitioner: Partitioner)(implicit m: Monoid[V]): RDD[(K, V)] = rdd.reduceByKey(partitioner, m.combine _)
    def sumByKey(numPartitions: NumPartitions)(implicit m: Monoid[V]): RDD[(K, V)] = rdd.reduceByKey(m.combine _, numPartitions)
  }
}
