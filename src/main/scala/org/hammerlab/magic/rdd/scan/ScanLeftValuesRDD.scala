package org.hammerlab.magic.rdd.scan

import cats.Monoid
import org.apache.spark.rdd.RDD
import magic_rdds.scan._

import scala.reflect.ClassTag

trait ScanLeftValuesRDD {

  /**
   * Expose methods on paired-RDDs for performing a scan-left over the values.
   *
   * See [[ScanLeftRDD]] / [[ScanValuesRDD]] for more discussion.
   */
  implicit class ScanLeftValuesRDDOps[K, V: ClassTag](rdd: RDD[(K, V)]) {
    // Dummy key "identity" value, not exposed in returned RDD
    private var k: K = _

    def scanLeftValues(implicit m: Monoid[V]): ScanValuesRDD[K, V] = scanLeftValues(m.empty)(m.combine)
    def scanLeftValues(identity: V)(combine: (V, V) ⇒ V): ScanValuesRDD[K, V] = scanLeftValues[V](identity)(combine)(combine)
    def scanLeftValues[W: ClassTag](identity: W)(aggregate: (W, V) ⇒ W)(combine: (W, W) ⇒ W): ScanValuesRDD[K, W] = scanLeftValues(identity, aggregate, combine, includeCurrentValue = false)

    def scanLeftValuesInclusive(implicit m: Monoid[V]): ScanValuesRDD[K, V] = scanLeftValuesInclusive(m.empty)(m.combine)
    def scanLeftValuesInclusive(identity: V)(combine: (V, V) ⇒ V): ScanValuesRDD[K, V] = scanLeftValuesInclusive[V](identity)(combine)(combine)
    def scanLeftValuesInclusive[W: ClassTag](identity: W)(aggregate: (W, V) ⇒ W)(combine: (W, W) ⇒ W): ScanValuesRDD[K, W] = scanLeftValues(identity, aggregate, combine, includeCurrentValue = true)

    def scanLeftValues[W: ClassTag](identity: W,
                                    aggregate: (W, V) ⇒ W,
                                    combine: (W, W) ⇒ W,
                                    includeCurrentValue: Boolean
    ): ScanValuesRDD[K, W] = {
      val ScanRDD(scanRDD, bounds, total) =
        rdd
        .scanLeftInclusive[(K, W, W)](
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
            }
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
