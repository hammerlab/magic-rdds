package org.hammerlab.magic.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.KeyPartitioner

import scala.reflect.ClassTag

object RDDUtil {
  def apply[T: ClassTag](partitions: Seq[Iterable[T]])(implicit sc: SparkContext): RDD[T] = {
    sc
      .parallelize(
        for {
          (elems, partition) <- partitions.zipWithIndex
          (elem, idx) <- elems.zipWithIndex
        } yield {
          (partition, idx) -> elem
        }
      )
      .repartitionAndSortWithinPartitions(KeyPartitioner(partitions.size))
      .values
  }
}
