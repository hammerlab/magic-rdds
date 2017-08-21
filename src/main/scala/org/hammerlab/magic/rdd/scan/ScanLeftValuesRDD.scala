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

    def scanLeftValues(includeCurrentValue: Boolean = false)(implicit m: Monoid[V]): ScanValuesRDD[K, V] =
      scanLeftValues(
        m.empty,
        includeCurrentValue
      )(
        m.combine
      )

    def scanLeftValues(identity: V,
                       includeCurrentValue: Boolean
                      )(
        combine: (V, V) ⇒ V
    ): ScanValuesRDD[K, V] =
      scanLeftValues[V](
        identity,
        combine,
        combine,
        includeCurrentValue
      )

    def scanLeftValues[W: ClassTag](identity: W,
                                    aggregate: (W, V) ⇒ W,
                                    combine: (W, W) ⇒ W,
                                    includeCurrentValue: Boolean
    ): ScanValuesRDD[K, W] = {
      val ScanRDD(scanRDD, bounds, total) =
        rdd
          .scanLeft[(K, W, W)](
            (k, identity, identity),
            {
              case ((_, _, prevW2), (k, v)) ⇒
                (
                  k,
                  prevW2,
                  aggregate(prevW2, v)
                )
            },
            {
              case ((_, _, prevW), (k, w1, w2)) ⇒
                (
                  k,
                  combine(prevW, w1),
                  combine(prevW, w2)
                )
            },
            includeCurrentValue = true
          )

      val project: ((K, W, W)) ⇒ (K, W) =
        {
          case (k, exclusiveW, inclusiveW) ⇒
            k →
              (
                if (includeCurrentValue)
                  inclusiveW
                else
                  exclusiveW
              )
        }

      ScanValuesRDD(
        scanRDD.map(project),
        bounds.map(project(_)._2),
        project(total)._2
      )
    }
  }
}
