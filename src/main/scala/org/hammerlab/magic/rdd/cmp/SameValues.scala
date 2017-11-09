package org.hammerlab.magic.rdd.cmp

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait SameValues {
  /**
   * Wrap a paired-[[RDD]] and expose methods for comparing its elements to another [[RDD]]'s, per-key, disregarding the
   * order in which they appear in each.
   */
  implicit class SameValuesOps[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) extends Serializable {

    /**
     * Compare [[rdd]] with another [[RDD]] by considering how many keys have the same values in each, or are only present
     * in one or the other.
     */
    def compareByKey(o: RDD[(K, V)]): Keyed[K, V] = Keyed(rdd, o)
  }
}

