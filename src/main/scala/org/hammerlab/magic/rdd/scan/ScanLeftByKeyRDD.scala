package org.hammerlab.magic.rdd.scan

import cats.Monoid
import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.scan.ScanLeftRDD._

import scala.reflect.ClassTag

case class ScanLeftByKeyRDD[K: ClassTag, V: ClassTag](@transient rdd: RDD[(K, V)]) {

  // Dummy key value, not exposed in returned RDD
  private var k: K = _

  def scanLeftByKey(implicit m: Monoid[V]): RDD[(K, V)] =
    scanLeftByKey(
      m.empty
    )(
      m.combine
    )

  def scanLeftByKey(identity: V)(combine: (V, V) ⇒ V): RDD[(K, V)] =
    rdd
      .scanLeft(
        (k, identity)
      ) {
        case (
          (_, sum),
          (k, v)
        ) ⇒
          k →
            combine(
              sum,
              v
            )
      }
}

object ScanLeftByKeyRDD {
  implicit def toScanLeftByKeyRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): ScanLeftByKeyRDD[K, V] =
    ScanLeftByKeyRDD(rdd)
}
