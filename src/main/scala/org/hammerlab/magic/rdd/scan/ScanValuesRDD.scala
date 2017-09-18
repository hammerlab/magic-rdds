package org.hammerlab.magic.rdd.scan

import org.apache.spark.rdd.RDD

/**
 * Analogue of [[ScanRDD]] for scans over the values of paired RDDs.
 *
 * See the [[ScanRDD]] for important discussions of field-semantics.
 */
case class ScanValuesRDD[K, V](rdd: RDD[(K, V)],
                               partitionPrefixes: Array[V],
                               total: V)

object ScanValuesRDD {
  implicit def unwrapScanValuesRDD[K, V](rdd: ScanValuesRDD[K, V]): RDD[(K, V)] = rdd.rdd
}
