package org.hammerlab.magic.rdd.scan

import org.apache.spark.rdd.RDD

case class ScanValuesRDD[K, V](rdd: RDD[(K, V)],
                               partitionTotals: Array[V],
                               total: V)

object ScanValuesRDD {
  implicit def unwrapScanValuesRDD[K, V](rdd: ScanValuesRDD[K, V]): RDD[(K, V)] = rdd.rdd
}
