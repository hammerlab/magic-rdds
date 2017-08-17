package org.hammerlab.magic.rdd.scan

import cats.Monoid
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object ScanLeftRDD {

  /**
   * RDD wrapper supporting methods that compute partial-sums across the RDD.
   *
   * See [[ScanRDD]] for some discussion of the return value's semantics.
   */
  implicit class ScanLeftRDDOps[T: ClassTag](rdd: RDD[T]) {
    def scanLeft(implicit m: Monoid[T]): ScanRDD[T] = scanLeft(m.empty)(m.combine)

    def scanLeft(identity: T)(combine: (T, T) ⇒ T): ScanRDD[T] =
      scanLeft(identity, combine, combine)

    def scanLeft[U: ClassTag](identity: U,
                              aggregate: (U, T) ⇒ U,
                              combine: (U, U) ⇒ U): ScanRDD[U] = {
      val numPartitions = rdd.getNumPartitions
      val (partitionSums, total) = {
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

      val partitionSumsRDD =
        rdd
          .sparkContext
          .parallelize(
            partitionSums,
            numPartitions
          )

      ScanRDD(
        rdd
          .zipPartitions(partitionSumsRDD) {
            (it, sumIter) ⇒
              it
              .scanLeft(sumIter.next)(aggregate)
              .drop(1)
          },
        partitionSums,
        total
      )
    }
  }
}
