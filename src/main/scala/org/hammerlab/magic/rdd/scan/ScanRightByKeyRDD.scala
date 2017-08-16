package org.hammerlab.magic.rdd.scan

import cats.Monoid
import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.scan.ScanRightRDD._

import scala.reflect.ClassTag

case class ScanRightByKeyRDD[K: ClassTag, V: ClassTag](@transient rdd: RDD[(K, V)]) {

  // Dummy key value, not exposed in returned RDD
  private var k: K = _

  def scanRightByKey(useRDDReversal: Boolean = false)(implicit m: Monoid[V]): RDD[(K, V)] =
    scanRightByKey(
      m.empty,
      useRDDReversal
    )(
      m.combine
    )

  def scanRightByKey(identity: V,
                     useRDDReversal: Boolean)(
      combine: (V, V) ⇒ V
  ): RDD[(K, V)] =
    rdd
      .scanRight(
        (k, identity),
        useRDDReversal
      ) {
        case (
          (k, v),
          (_, sum)
        ) ⇒
          k →
            combine(
              v,
              sum
            )
      }
}

object ScanRightByKeyRDD {
  implicit def toScanRightByKeyRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): ScanRightByKeyRDD[K, V] =
    ScanRightByKeyRDD(rdd)
}
