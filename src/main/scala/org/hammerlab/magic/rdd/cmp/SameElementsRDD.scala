package org.hammerlab.magic.rdd.cmp

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Wrap an [[RDD]] and expose methods for comparing its elements to those of other [[RDD]]'s, disregarding the order in
 * which they appear in each.
 */
class SameElementsRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {

  /**
   * Compare [[rdd]] with another [[RDD]] by considering how many keys have the same values in each, or are only present
   * in one or the other.
   */
  def compareByKey(o: RDD[(K, V)]): Cmp[K, V] = Cmp(rdd, o)

  /**
   * Return true iff [[rdd]] and another [[RDD]] have the same (key, value) pairs, disregarding whether they're in the
   * same order.
   */
  def sameElements(o: RDD[(K, V)]): Boolean = {
    compareByKey(o).isEqual
  }
}

object SameElementsRDD {
  implicit def rddToSameElementsRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): SameElementsRDD[K, V] =
    new SameElementsRDD(rdd)
}

