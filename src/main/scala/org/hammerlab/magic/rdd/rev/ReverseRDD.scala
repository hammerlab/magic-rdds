package org.hammerlab.magic.rdd.rev

import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.partitions.PartitionByKeyRDD._

import scala.reflect.ClassTag

class ReverseRDD[T: ClassTag](rdd: RDD[T]) extends Serializable {
  def reverse(preservePartitioning: Boolean = false): RDD[T] = {
    val numPartitions = rdd.getNumPartitions
    val keyedRDD =
      rdd
        .mapPartitionsWithIndex(
          (partitionIdx, it) ⇒
            for {
              (elem, idx) ← it.zipWithIndex
            } yield
              (numPartitions - 1 - partitionIdx, -idx) → elem
        )

    if (preservePartitioning)
      keyedRDD
        .partitionByKey(numPartitions)
    else
      keyedRDD
        .sortByKey()
        .values
  }
}

object ReverseRDD {
  implicit def makeReverseRDD[T: ClassTag](rdd: RDD[T]): ReverseRDD[T] = new ReverseRDD(rdd)
}
