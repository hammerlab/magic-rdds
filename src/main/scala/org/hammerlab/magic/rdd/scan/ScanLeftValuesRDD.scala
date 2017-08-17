package org.hammerlab.magic.rdd.scan

import cats.Monoid
import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.scan.ScanLeftRDD._

import scala.reflect.ClassTag

object ScanLeftValuesRDD {

  /**
   * Expose methods on paired-RDDs for performing a scan-left over the values.
   *
   * See [[ScanLeftRDD]] / [[ScanValuesRDD]] for more discussion.
   */
  implicit class ScanLeftValuesRDDOps[K, V: ClassTag](rdd: RDD[(K, V)]) {
    // Dummy key "identity" value, not exposed in returned RDD
    private var k: K = _

    def scanLeftValues(implicit m: Monoid[V]): ScanValuesRDD[K, V] =
      scanLeftValues(
        m.empty
      )(
        m.combine
      )

    def scanLeftValues(identity: V)(combine: (V, V) ⇒ V): ScanValuesRDD[K, V] = {
      val ScanRDD(scanRDD, bounds, total) =
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

      ScanValuesRDD(
        scanRDD,
        bounds.map(_._2),
        total._2
      )
    }
  }
}
