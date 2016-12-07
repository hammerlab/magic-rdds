package org.hammerlab.magic.rdd.scan

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * RDD wrapper supporting methods that compute partial-sums across the RDD.
 */
class ScanLeftRDD[T: ClassTag](@transient val rdd: RDD[T]) extends Serializable {

  def scanLeft(identity: T)(combine: (T, T) ⇒ T): RDD[T] =
    scanLeft(identity, combine, combine)

  def scanLeft[U: ClassTag](identity: U, aggregate: (U, T) ⇒ U, combine: (U, U) ⇒ U): RDD[U] = {
    val numPartitions = rdd.getNumPartitions
    val partitionSums =
      rdd
        .mapPartitionsWithIndex(
          (idx, it) ⇒
            if (idx + 1 == numPartitions)
              Iterator()
            else
              Iterator(
                it.foldLeft(identity)(aggregate)
              )
        )
        .collect()
        .scanLeft(identity)(combine)

    val partitionSumsRDD =
      rdd
        .sparkContext
        .parallelize(partitionSums, numPartitions)

    rdd.zipPartitions(partitionSumsRDD)(
      (it, sumIter) ⇒ {
        it
          .scanLeft(sumIter.next)(aggregate)
          .drop(1)
      }
    )
  }
}

object ScanLeftRDD {
  implicit def toScanLeftRDD[T: ClassTag](rdd: RDD[T]): ScanLeftRDD[T] = new ScanLeftRDD(rdd)
}
