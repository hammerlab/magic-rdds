package org.hammerlab.magic.rdd.scan

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import org.hammerlab.iterator.DropRightIterator._
import org.hammerlab.magic.rdd.rev.ReverseRDD._
import ScanLeftRDD._

/**
 * RDD wrapper supporting methods that compute partial-sums (from right to left) across the RDD.
 */
class ScanRightRDD[T: ClassTag](@transient val rdd: RDD[T]) extends Serializable {

  /**
   *
   * @param identity used to initialize per-partition "sums"
   * @param aggregate aggregate an RDD element into a running "sum"
   * @param combine combine two "sums"
   * @param useRDDReversal when set, reverse the RDD, do a `scanLeft` (see [[ScanLeftRDD]], and then reverse it back.
   *                       Usually this will be more work than the default implementation, but the latter materializes
   *                       whole partitions into memory as part of calling [[Iterator.scanRight]], which may be
   *                       prohibitively memory-expensive in some cases.
   * @return
   */
  def scanRight[U: ClassTag](identity: U,
                             aggregate: (T, U) ⇒ U,
                             combine: (U, U) ⇒ U,
                             useRDDReversal: Boolean): RDD[U] =
    if (useRDDReversal)
      rdd
        .reverse()
        .scanLeft[U](identity, (u, t) ⇒ aggregate(t, u), (u1, u2) ⇒ combine(u2, u1))
        .reverse()
    else {
      val numPartitions = rdd.getNumPartitions
      val partitionSums =
        rdd
          .mapPartitionsWithIndex(
            (idx, it) ⇒
              if (idx == 0)
                Iterator()
              else
                Iterator(
                  // Since we only want each partition's sum here, we can optimize by using `foldLeft` instead of
                  // `foldRight`; the latter materializes the entire partition-iterator into memory in order to traverse
                  // through it in reverse order.
                  // Note that we still have to do the latter below.
                  it.foldLeft(identity)((sum, elem) ⇒ aggregate(elem, sum))
                )
          )
          .collect()
          .scanRight(identity)(combine)

      val partitionSumsRDD =
        rdd
          .sparkContext
          .parallelize(partitionSums, numPartitions)

      rdd.zipPartitions(partitionSumsRDD)(
        (it, sumIter) ⇒ {
          it
            .scanRight(sumIter.next)(aggregate)
            .dropRight(1)
        }
      )
    }

  def scanRight[U: ClassTag](identity: U,
                             aggregate: (T, U) ⇒ U,
                             combine: (U, U) ⇒ U): RDD[U] =
    scanRight(identity, aggregate, combine, useRDDReversal = false)

  def scanRight(identity: T, useRDDReversal: Boolean = false)(combine: (T, T) ⇒ T): RDD[T] =
    scanRight(identity, combine, combine, useRDDReversal)
}

object ScanRightRDD {
  implicit def toScanRightRDD[T: ClassTag](rdd: RDD[T]): ScanRightRDD[T] = new ScanRightRDD(rdd)
}
