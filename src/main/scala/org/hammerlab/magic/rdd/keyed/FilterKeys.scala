package org.hammerlab.magic.rdd.keyed

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait FilterKeys {
  implicit class FilterKeysOps[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) extends Serializable {
    def filterKeys(setBroadcast: Broadcast[Set[K]]): RDD[(K, V)] =
      rdd
        .filter {
          case (k, _) ⇒
            setBroadcast.value(k)
        }
  }
}
