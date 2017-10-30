package org.hammerlab.magic.rdd.scan

import cats.Monoid
import magic_rdds.scan._
import org.apache.spark.rdd.RDD
import org.hammerlab.iterator.DropRightIterator._
import org.hammerlab.magic.rdd.rev.ReverseRDD._
import org.hammerlab.magic.rdd.zip.ZipPartitionsRDD._

import scala.reflect.ClassTag

/**
 * RDD wrapper supporting methods that compute partial-sums (from right to left) across the RDD.
 *
 * Callers should be aware of one implementation detail: by default, scan-rights proceed by reversing the RDD,
 * performing a scan-left, then reversing the result, which involves 3 Spark jobs.
 *
 * An alternative implementation delegates to [[scala.collection.Iterator.scanRight]], which is likely less expensive,
 * but materializes whole partitions into memory, which is generally a severe anti-pattern in Spark computations.
 */
trait ScanRightRDD {

  implicit class ScanRightRDDOps[T: ClassTag](rdd: RDD[T]) {

    def scanRight(includeCurrentValue: Boolean = false,
                  useRDDReversal: Boolean = true
                 )(
        implicit m: Monoid[T]
    ): ScanRDD[T] =
      scanRight[T](
        includeCurrentValue,
        useRDDReversal,
        m.combine _
      )

    def scanRight[U: ClassTag](includeCurrentValue: Boolean,
                               useRDDReversal: Boolean,
                               aggregate: (T, U) ⇒ U
                              )(
        implicit m: Monoid[U]
    ): ScanRDD[U] =
      scanRight(
        aggregate,
        includeCurrentValue = includeCurrentValue,
        useRDDReversal = useRDDReversal
      )

    def scanRight[U: ClassTag](aggregate: (T, U) ⇒ U,
                               includeCurrentValue: Boolean,
                               useRDDReversal: Boolean)(
        implicit m: Monoid[U]
    ): ScanRDD[U] =
      scanRight(
        m.empty,
        aggregate,
        m.combine,
        includeCurrentValue,
        useRDDReversal
      )

    /**
     *
     * @param identity used to initialize per-partition "sums"
     * @param aggregate aggregate an RDD element into a running "sum"
     * @param combine combine two "sums"
     * @param useRDDReversal when set, reverse the RDD, do a `scanLeft` (see [[ScanLeftRDD]], and then reverse it back.
     *                       Usually this will be more work than the default implementation, but the latter materializes
     *                       whole partitions into memory as part of calling [[scala.collection.Iterator.scanRight]],
     *                       which may be prohibitively memory-expensive in some cases.
     * @return
     */
    def scanRight[U: ClassTag](identity: U,
                               aggregate: (T, U) ⇒ U,
                               combine: (U, U) ⇒ U,
                               includeCurrentValue: Boolean,
                               useRDDReversal: Boolean): ScanRDD[U] =
      if (useRDDReversal) {
        val ScanRDD(scanRDD, bounds, total) =
          rdd
            .reverse()
            .scanLeft[U](
              identity,
              (u, t) ⇒ aggregate(t, u),
              (u1, u2) ⇒ combine(u2, u1),
              includeCurrentValue
            )
          ScanRDD(
            scanRDD.reverse(),
            bounds.reverse,
            total
          )
      } else {
        val numPartitions = rdd.getNumPartitions
        val (partitionSuffixes, total) = {
          val sums =
            rdd
              .mapPartitionsWithIndex(
                (idx, it) ⇒
                  Iterator(
                    // Since we only want each partition's sum here, we can optimize by using `foldLeft` instead of
                    // `foldRight`; the latter materializes the entire partition-iterator into memory in order to traverse
                    // through it in reverse order.
                    // Note that we still have to do the latter below.
                    idx →
                      it
                        .foldLeft(
                          identity
                        )(
                          (sum, elem) ⇒
                            aggregate(elem, sum)
                        )
                  )
              )
              .collect()
              .map(_._2)
              .scanRight(identity)(combine)
              .iterator

          val total = sums.next

          (
            sums ++ Iterator(identity) toArray,
            total
          )
        }

        val partitionSumsRDD =
          rdd
            .sparkContext
            .parallelize(
              partitionSuffixes,
              numPartitions
            )

        ScanRDD(
          rdd
            .zippartitions(
              partitionSumsRDD
            ) {
              (it, suffixIter) ⇒
                val scanned =
                  it
                    .scanRight(
                      suffixIter.next
                    )(
                      aggregate
                    )

                if (includeCurrentValue)
                  scanned.dropRight(1)
                else
                  scanned.drop(1)
            },
          partitionSuffixes,
          total
        )
      }

    def scanRight(identity: T,
                  includeCurrentValue: Boolean,
                  useRDDReversal: Boolean)(
        combine: (T, T) ⇒ T
    ): ScanRDD[T] =
      scanRight(
        identity,
        combine,
        combine,
        includeCurrentValue,
        useRDDReversal
      )

  }
}
