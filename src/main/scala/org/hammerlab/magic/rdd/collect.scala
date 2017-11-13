package org.hammerlab.magic.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait collect {
  implicit class CollectPartitionsOps[T: ClassTag](rdd: RDD[T]) extends Serializable {
    def collectParts: Array[Array[T]] =
      rdd
        .sparkContext
        .runJob(
          rdd,
          (iter: Iterator[T]) ⇒ iter.toArray
        )

    def collectPartitions(): Array[Array[T]] =
      rdd
        .mapPartitions(
          it ⇒
            Iterator(
              it.toArray
            )
        )
        .collect()
  }
}
