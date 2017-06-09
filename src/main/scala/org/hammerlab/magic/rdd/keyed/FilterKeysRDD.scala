package org.hammerlab.magic.rdd.keyed

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class FilterKeysRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
  def filterKeys(setBroadcast: Broadcast[Set[K]]): RDD[(K, V)] =
    rdd
      .filter {
        case (k, _) ⇒
          setBroadcast.value(k)
      }
}

object FilterKeysRDD {
  implicit def makeFilterKeysRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): FilterKeysRDD[K, V] = FilterKeysRDD(rdd)
}
