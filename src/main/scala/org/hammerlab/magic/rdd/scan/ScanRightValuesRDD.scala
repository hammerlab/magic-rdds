package org.hammerlab.magic.rdd.scan

import cats.Monoid
import magic_rdds.scan._
import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.scan.ScanRightRDD.UseRDDReversal

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

    /** `scanRightValues` using a [[cats.Monoid]], optionally  the RDD vs materializing whole partitions */

    def scanRightValues(implicit m: Monoid[V]): ScanValuesRDD[K, V] = scanRightValues(useRDDReversal = true)
    def scanRightValues(useRDDReversal: Boolean)(implicit m: Monoid[V]): ScanValuesRDD[K, V] = scanRightValues(m.empty, useRDDReversal = useRDDReversal)(m.combine)

    /** `scanRightValues` where the input type matches the output type */

    def scanRightValues(identity: V)(combine: (V, V) ⇒ V): ScanValuesRDD[K, V] = scanRightValues[V](identity)(combine)(combine)
    def scanRightValues(identity: V, useRDDReversal: Boolean)(combine: (V, V) ⇒ V): ScanValuesRDD[K, V] = scanRightValues[V](identity, useRDDReversal = useRDDReversal)(combine)(combine)

    /** scanRightValues where the input type may not match the output type */

    def scanRightValues[W: ClassTag](identity: W)(aggregate: (V, W) ⇒ W)(combine: (W, W) ⇒ W): ScanValuesRDD[K, W] = scanRightValues[W](identity, aggregate, combine, useRDDReversal = false, includeCurrentValue = false)
    def scanRightValues[W: ClassTag](identity: W, useRDDReversal: Boolean)(aggregate: (V, W) ⇒ W)(combine: (W, W) ⇒ W): ScanValuesRDD[K, W] = scanRightValues[W](identity, aggregate, combine, useRDDReversal = useRDDReversal, includeCurrentValue = false)

    /** `scanRightValuesInclusive`: include each element in the sum that replaces it */

    def scanRightValuesInclusive(implicit m: Monoid[V]): ScanValuesRDD[K, V] = scanRightValuesInclusive(m.empty)(m.combine)
    def scanRightValuesInclusive(useRDDReversal: Boolean)(implicit m: Monoid[V]): ScanValuesRDD[K, V] = scanRightValuesInclusive(m.empty, useRDDReversal = true)(m.combine)

    def scanRightValuesInclusive(identity: V)(combine: (V, V) ⇒ V): ScanValuesRDD[K, V] = scanRightValuesInclusive[V](identity)(combine)(combine)
    def scanRightValuesInclusive(identity: V, useRDDReversal: Boolean)(combine: (V, V) ⇒ V): ScanValuesRDD[K, V] = scanRightValuesInclusive[V](identity, useRDDReversal = useRDDReversal)(combine)(combine)

    def scanRightValuesInclusive[W: ClassTag](identity: W)(aggregate: (V, W) ⇒ W)(combine: (W, W) ⇒ W): ScanValuesRDD[K, W] = scanRightValues[W](identity, aggregate, combine, useRDDReversal = false, includeCurrentValue = true)
    def scanRightValuesInclusive[W: ClassTag](identity: W, useRDDReversal: Boolean)(aggregate: (V, W) ⇒ W)(combine: (W, W) ⇒ W): ScanValuesRDD[K, W] = scanRightValues[W](identity, aggregate, combine, useRDDReversal = useRDDReversal, includeCurrentValue = true)

    /** General implementation */

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
