package org.hammerlab.magic.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class CappedGroupByKeyRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
  def cappedGroupByKey(maxPerKey: Int): RDD[(K, Vector[V])] = {
    rdd.combineByKey[Vector[V]](
      (e: V) => Vector(e),
      (v: Vector[V], e: V) => {
        if (v.length >= maxPerKey)
          v
        else
          v :+ e
      },
      (v1: Vector[V], v2: Vector[V]) => {
        if (v1.length >= maxPerKey)
          v1
        else if (v1.length + v2.length >= maxPerKey)
          v1 ++ v2.take(maxPerKey - v1.length)
        else
          v1 ++ v2
      },
      rdd.getNumPartitions
    )
  }
}

object CappedGroupByKeyRDD {
  implicit def rddToCappedGroupByKeyRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): CappedGroupByKeyRDD[K, V] =
    new CappedGroupByKeyRDD(rdd)
}
