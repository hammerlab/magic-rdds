package org.hammerlab.magic.rdd.scan

import cats.Monoid
import hammerlab.iterator._
import magic_rdds._
import org.apache.spark.rdd.RDD

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

    // Monoid-based scans

    def scanRight()(implicit m: Monoid[T]): ScanRDD[T] = scanRight(useRDDReversal = true)
    def scanRight(useRDDReversal: Boolean)(implicit m: Monoid[T]): ScanRDD[T] =
      scanRight(
        m.empty,
        useRDDReversal = useRDDReversal
      )(
        m.combine
      )

    def scanRightInclusive()(implicit m: Monoid[T]): ScanRDD[T] = scanRightInclusive(useRDDReversal = true)
    def scanRightInclusive(useRDDReversal: Boolean)(implicit m: Monoid[T]): ScanRDD[T] =
      scanRightInclusive(
        m.empty,
        useRDDReversal = useRDDReversal
      )(
        m.combine
      )

    // Output type == input type

    def scanRight(identity: T)(combine: (T, T) ⇒ T): ScanRDD[T] = scanRight(identity, useRDDReversal = true)(combine)
    def scanRight(identity: T, useRDDReversal: Boolean)(combine: (T, T) ⇒ T): ScanRDD[T] =
      scanRight[T](
        identity,
        useRDDReversal
      )(
        combine
      )(
        combine
      )
    def scanRightInclusive(identity: T)(combine: (T, T) ⇒ T): ScanRDD[T] = scanRightInclusive(identity, useRDDReversal = true)(combine)
    def scanRightInclusive(identity: T, useRDDReversal: Boolean)(combine: (T, T) ⇒ T): ScanRDD[T] =
      scanRightInclusive[T](
        identity,
        useRDDReversal
      )(
        combine
      )(
        combine
      )

    // Output type != input type

    def scanRight[U: ClassTag](identity: U)(aggregate: (T, U) ⇒ U)(combine: (U, U) ⇒ U): ScanRDD[U] = scanRight(identity, useRDDReversal = true)(aggregate)(combine)
    def scanRight[U: ClassTag](identity: U, useRDDReversal: Boolean)(aggregate: (T, U) ⇒ U)(combine: (U, U) ⇒ U): ScanRDD[U] =
      scanRight[U](
        identity,
        aggregate,
        combine,
        includeCurrentValue = false,
        useRDDReversal = useRDDReversal
      )
    def scanRightInclusive[U: ClassTag](identity: U)(aggregate: (T, U) ⇒ U)(combine: (U, U) ⇒ U): ScanRDD[U] = scanRightInclusive(identity, useRDDReversal = true)(aggregate)(combine)
    def scanRightInclusive[U: ClassTag](identity: U, useRDDReversal: Boolean)(aggregate: (T, U) ⇒ U)(combine: (U, U) ⇒ U): ScanRDD[U] =
      scanRight[U](
        identity,
        aggregate,
        combine,
        includeCurrentValue = true,
        useRDDReversal = useRDDReversal
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
          .scanLeftImpl[U](
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
                  scanned.dropright(1)
                else
                  scanned.drop(1)
            },
          partitionSuffixes,
          total
        )
      }
  }
}
