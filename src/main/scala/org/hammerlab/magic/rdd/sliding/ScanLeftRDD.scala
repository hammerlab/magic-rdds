package org.hammerlab.magic.rdd.sliding

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * RDD wrapper supporting methods that compute partial-sums across the RDD.
 */
class ScanLeftRDD[T: ClassTag](@transient val rdd: RDD[T]) extends Serializable {

  def scanLeft(zero: T)(combine: (T, T) ⇒ T): RDD[T] =
    scanLeft(zero, combine, combine)

  def scanLeft[U: ClassTag](zero: U, aggregate: (U, T) ⇒ U, combine: (U, U) ⇒ U): RDD[U] = {
    val numPartitions = rdd.getNumPartitions
    val partitionSums =
      rdd
        .mapPartitionsWithIndex(
          (idx, it) ⇒
            if (idx + 1 == numPartitions)
              Iterator()
            else
              Iterator(
                it.foldLeft(zero)(aggregate)
              )
        )
        .collect()
        .scanLeft(zero)(combine)

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
  implicit def make[T: ClassTag](rdd: RDD[T]): ScanLeftRDD[T] = new ScanLeftRDD(rdd)
}
