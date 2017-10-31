package org.hammerlab.magic.rdd.keyed

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait CappedGroupByKey {
  /**
   * Wrap an [[RDD]] and expose a `cappedGroupByKey` method, which behaves like
   * [[org.apache.spark.rdd.PairRDDFunctions.groupByKey]] but with a cap on the number of values that will be accumulated
   * for each key.
   *
   * Takes the first values for each key, discarding the rest; to obtain a random sampling of the elements for each key,
   * see [[SampleByKey]].
   */
  implicit class CappedGroupByKeyOps[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) extends Serializable {
    def cappedGroupByKey(maxPerKey: Int): RDD[(K, Vector[V])] =
      rdd.combineByKey[Vector[V]](
        (e: V) ⇒ Vector(e),
        (v: Vector[V], e: V) ⇒ {
          if (v.length >= maxPerKey)
            v
          else
            v :+ e
        },
        (v1: Vector[V], v2: Vector[V]) ⇒ {
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
