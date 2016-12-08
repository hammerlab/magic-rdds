package org.hammerlab.magic.rdd.scan

import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.scan.ScanLeftRDD._

import scala.reflect.ClassTag

class ScanLeftByKeyRDD[K: ClassTag, V: ClassTag](@transient val rdd: RDD[(K, V)]) extends Serializable {
  var k: K = _
  def scanLeftByKey(identity: V)(combine: (V, V) ⇒ V): RDD[(K, V)] = {
    rdd.scanLeft((k, identity))((sum, elem) ⇒ (elem._1, combine(sum._2, elem._2)))
  }
}

object ScanLeftByKeyRDD {
  implicit def toScanLeftByKeyRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): ScanLeftByKeyRDD[K, V] =
    new ScanLeftByKeyRDD(rdd)
}
