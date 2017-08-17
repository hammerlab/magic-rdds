package org.hammerlab.magic.rdd.scan

import cats.Monoid
import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.scan.ScanRightRDD._

import scala.reflect.ClassTag

object ScanRightValuesRDD {

  /**
   * Expose methods on paired-RDDs for performing a scan-right over the values.
   *
   * See [[ScanRightRDD]] / [[ScanValuesRDD]] for more discussion.
   */
  implicit class ScanRightValuesRDDOps[K, V: ClassTag](rdd: RDD[(K, V)]) {
    // Dummy key value, not exposed in returned RDD
    private var k: K = _

    def scanRightValues(useRDDReversal: Boolean = false)(implicit m: Monoid[V]): ScanValuesRDD[K, V] =
      scanRightValues(
        m.empty,
        useRDDReversal
      )(
        m.combine
      )

    def scanRightValues(identity: V,
                        useRDDReversal: Boolean)(
        combine: (V, V) ⇒ V
    ): ScanValuesRDD[K, V] = {
      val ScanRDD(scanRDD, bounds, total) =
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

      ScanValuesRDD(
        scanRDD,
        bounds.map(_._2),
        total._2
      )
    }
  }
}
