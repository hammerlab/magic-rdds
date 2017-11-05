package org.hammerlab.magic.rdd.scan

import cats.Monoid
import hammerlab.iterator._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait ScanLeftRDD {

  /**
   * RDD wrapper supporting methods that compute partial-sums across the RDD.
   *
   * See [[ScanRDD]] for some discussion of the return value's semantics.
   */
  implicit class ScanLeftRDDOps[T: ClassTag](rdd: RDD[T]) {
    def scanLeft(includeCurrentValue: Boolean = false)(
        implicit m: Monoid[T]
    ): ScanRDD[T] =
      scanLeft(
        m.empty,
        includeCurrentValue
      )(
        m.combine
      )

    def scanLeft(implicit m: Monoid[T]): ScanRDD[T] =
      scanLeft(includeCurrentValue = false)

    def scanLeftInclusive(implicit m: Monoid[T]): ScanRDD[T] =
      scanLeft(includeCurrentValue = true)

    def scanLeft(identity: T,
                 includeCurrentValue: Boolean
    )(
        combine: (T, T) ⇒ T
    ): ScanRDD[T] =
      scanLeft(
        identity,
        combine,
        combine,
        includeCurrentValue
      )

    def scanLeft[U: ClassTag](identity: U,
                              aggregate: (U, T) ⇒ U,
                              combine: (U, U) ⇒ U,
                              includeCurrentValue: Boolean): ScanRDD[U] = {
      val numPartitions = rdd.getNumPartitions
      val (partitionPrefixes, total) = {
        val sums =
          rdd
            .mapPartitionsWithIndex(
              (idx, it) ⇒
                Iterator(
                  idx →
                    it.foldLeft(identity)(aggregate)
                )
            )
            .collect()
            .map(_._2)
            .scanLeft(identity)(combine)

        val total = sums(numPartitions)

        (sums.dropRight(1), total)
      }

      val partitionPrefixesRDD =
        rdd
          .sparkContext
          .parallelize(
            partitionPrefixes,
            numPartitions
          )

      ScanRDD(
        rdd
          .zipPartitions(partitionPrefixesRDD) {
            (it, prefixIter) ⇒
              val scanned =
                it
                  .scanLeft(
                    prefixIter.next
                  )(
                    aggregate
                  )

              if (includeCurrentValue)
                scanned.drop(1)
              else
                scanned.dropright(1)
          },
        partitionPrefixes,
        total
      )
    }
  }
}
