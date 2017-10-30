package org.hammerlab.magic.rdd.scan

import cats.Monoid
import magic_rdds.scan._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait ScanRightValuesRDD {

  /**
   * Expose methods on paired-RDDs for performing a scan-right over the values.
   *
   * See [[ScanRightRDD]] / [[ScanValuesRDD]] for more discussion.
   */
  implicit class ScanRightValuesRDDOps[K, V: ClassTag](rdd: RDD[(K, V)]) {
    // Dummy key value, not exposed in returned RDD
    private var k: K = _

    def scanRightValues(includeCurrentValue: Boolean = false,
                        useRDDReversal: Boolean = false
                       )(
        implicit m: Monoid[V]
    ): ScanValuesRDD[K, V] =
      scanRightValues(
        m.empty,
        useRDDReversal,
        includeCurrentValue
      )(
        m.combine
      )

    def scanRightValues(identity: V,
                        useRDDReversal: Boolean,
                        includeCurrentValue: Boolean)(
                           combine: (V, V) ⇒ V
                       ): ScanValuesRDD[K, V] =
      scanRightValues(
        identity,
        combine,
        combine,
        useRDDReversal,
        includeCurrentValue
      )

    def scanRightValues[W: ClassTag](identity: W,
                                     aggregate: (V, W) ⇒ W,
                                     combine: (W, W) ⇒ W,
                                     useRDDReversal: Boolean,
                                     includeCurrentValue: Boolean): ScanValuesRDD[K, W] = {

      val ag: ((K, V), (K, W, W)) ⇒ (K, W, W) =
      {
        case ((k, v), (_, _, prevW2)) ⇒
          (
            k,
            prevW2,
            aggregate(v, prevW2)
          )
      }

      val comb: ((K, W, W), (K, W, W)) ⇒ (K, W, W) =
      {
        case ((k, exclusiveW, inclusiveW), (_, _, prevW)) ⇒
          (
            k,
            combine(exclusiveW, prevW),
            combine(inclusiveW, prevW)
          )
      }

      val ScanRDD(scanRDD, bounds, total) =
        rdd
          .scanRight[(K, W, W)](
            (k, identity, identity),
            ag,
            comb,
            includeCurrentValue = true,
            useRDDReversal = useRDDReversal
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
