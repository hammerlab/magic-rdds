package org.hammerlab.magic.rdd.sort

import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}

import scala.reflect.ClassTag

class SortRDD[T : Ordering : ClassTag](@transient rdd: RDD[T]) extends Serializable {

  val ordering = implicitly[Ordering[T]]

  def sort(numPartitions: Int = rdd.partitions.length,
           ascending: Boolean = true): (RDD[T], RangePartitioner[T, Null]) = {

    val withNulls = rdd.map(_ -> null)
    val part = new RangePartitioner(numPartitions, withNulls, ascending)

    (
      new ShuffledRDD[T, Null, Null](withNulls, part)
        .setKeyOrdering(if (ascending) ordering else ordering.reverse)
        .keys,
      part
    )
  }
}

object SortRDD {
  implicit def rddToSortRDD[T : Ordering : ClassTag](rdd: RDD[T]): SortRDD[T] = new SortRDD(rdd)
}
