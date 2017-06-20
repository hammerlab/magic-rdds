package org.hammerlab.magic.rdd.zip

import org.apache.spark.rdd.RDD
import org.apache.spark.zip.{ZippedPartitionsBaseRDD, ZippedPartitionsPartition}
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

class ZippedPartitionsWithIndexRDD2[A: ClassTag, B: ClassTag, V: ClassTag](
    sc: SparkContext,
    var f: (Int, Iterator[A], Iterator[B]) ⇒ Iterator[V],
    var rdd1: RDD[A],
    var rdd2: RDD[B],
    preservesPartitioning: Boolean = false)
  extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2), preservesPartitioning) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(s.index, rdd1.iterator(partitions(0), context), rdd2.iterator(partitions(1), context))
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    f = null
  }
}
