package org.hammerlab.magic.rdd.partitions

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Wrap an [[RDD]] and expose a `collectPartitions` method that is similar to [[RDD.collect]], but returns an [[Array]]
 * of per-partition [[Array]]s.
 */
class CollectPartitionsRDD[T: ClassTag](rdd: RDD[T]) extends Serializable {
  def collectPartitions(): Array[Array[T]] = rdd.mapPartitions(it => Iterator(it.toArray)).collect()
}

object CollectPartitionsRDD {
  implicit def rddToCollectPartitionsRDD[T: ClassTag](rdd: RDD[T]): CollectPartitionsRDD[T] =
    new CollectPartitionsRDD(rdd)
}
