package org.hammerlab.magic.rdd

import magic_rdds.partitions._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait rev {
  implicit class ReverseOps[T: ClassTag](rdd: RDD[T]) extends Serializable {
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
}
