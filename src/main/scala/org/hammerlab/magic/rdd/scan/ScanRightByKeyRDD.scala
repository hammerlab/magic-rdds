package org.hammerlab.magic.rdd.scan

import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.scan.ScanRightRDD._

import scala.reflect.ClassTag

class ScanRightByKeyRDD[K: ClassTag, V: ClassTag](@transient val rdd: RDD[(K, V)]) extends Serializable {
  var k: K = _
  def scanRightByKey(identity: V, useRDDReversal: Boolean = false)(combine: (V, V) ⇒ V): RDD[(K, V)] = {
    rdd.scanRight((k, identity), useRDDReversal)((elem, sum) ⇒ (elem._1, combine(sum._2, elem._2)))
  }
}

object ScanRightByKeyRDD {
  implicit def toScanRightByKeyRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): ScanRightByKeyRDD[K, V] =
    new ScanRightByKeyRDD(rdd)
}
