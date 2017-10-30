package org.hammerlab.magic.rdd.sort

import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}

import scala.reflect.ClassTag

trait ops {
  implicit class SortRDDOps[T : Ordering : ClassTag](rdd: RDD[T]) {

    val ordering = implicitly[Ordering[T]]

    def sort(numPartitions: Int = rdd.partitions.length,
             ascending: Boolean = true): RDD[T] = {

      val withNulls = rdd.map(_ → null)
      val part = new RangePartitioner(numPartitions, withNulls, ascending)

      new ShuffledRDD[T, Null, Null](withNulls, part)
        .setKeyOrdering(if (ascending) ordering else ordering.reverse)
        .keys
    }
  }
}
