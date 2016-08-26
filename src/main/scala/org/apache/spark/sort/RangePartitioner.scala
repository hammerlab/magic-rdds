package org.apache.spark.sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{RangePartitioner => SparkRangePartitioner}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class RangePartitioner[K : Ordering : ClassTag, V](partitions: Int,
                                                   rdd: RDD[_ <: Product2[K, V]],
                                                   private var ascending: Boolean = true)
  extends SparkRangePartitioner(partitions, rdd, ascending)

object RangePartitioner {
  def sketch[K : ClassTag](rdd: RDD[K],
                           sampleSizePerPartition: Int): (Long, Array[(Int, Long, Array[K])]) =
    SparkRangePartitioner.sketch(rdd, sampleSizePerPartition)

  def determineBounds[K : Ordering : ClassTag](candidates: ArrayBuffer[(K, Float)],
                                               partitions: Int): Array[K] =
    SparkRangePartitioner.determineBounds(candidates, partitions)
}
