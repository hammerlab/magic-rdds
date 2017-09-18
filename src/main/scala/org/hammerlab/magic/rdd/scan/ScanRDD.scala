package org.hammerlab.magic.rdd.scan

import org.apache.spark.rdd.RDD

/**
 * Holds the result of a scan operation over an RDD
 *
 * @param rdd post-scan RDD; elements are replaced with the "total" up to *and including* themselves. This differs from
 *            scala collections' "scan" behavior, which emits an initial "identity" element.
 * @param partitionPrefixes the "sum" of all elements that precede this partition; here the first element is the
 *                          identity, consistent with scala collections' behavior, but the final "total" element is
 *                          moved over to the [[total]] field, so that this array has the same number of elements as
 *                          there are RDD partitions.
 * @param total the "sum" of all elements in the scanned RDD; Scala collections typically leave this appended to the
 *              result of a scan, but it is pulled out separately here.
 */
case class ScanRDD[T](rdd: RDD[T],
                      partitionPrefixes: Array[T],
                      total: T)

object ScanRDD {
  implicit def unwrapScanRDD[T](rdd: ScanRDD[T]): RDD[T] = rdd.rdd
}
