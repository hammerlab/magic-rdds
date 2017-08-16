package org.hammerlab.magic.rdd.scan

import org.apache.spark.rdd.RDD

case class ScanRDD[T](rdd: RDD[T],
                      partitionTotals: Array[T],
                      total: T)

object ScanRDD {
  implicit def unwrapScanRDD[T](rdd: ScanRDD[T]): RDD[T] = rdd.rdd
}
