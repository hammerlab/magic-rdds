package org.apache.spark.zip

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.{RDD, ZippedPartitionsBaseRDD, ZippedPartitionsPartition}

import scala.reflect.ClassTag

class ZippedPartitionsWithIndexRDD3
  [A: ClassTag, B: ClassTag, C: ClassTag, V: ClassTag](
    sc: SparkContext,
    var f: (Int, Iterator[A], Iterator[B], Iterator[C]) => Iterator[V],
    var rdd1: RDD[A],
    var rdd2: RDD[B],
    var rdd3: RDD[C],
    preservesPartitioning: Boolean = false)
  extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2, rdd3), preservesPartitioning) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(
      s.index,
      rdd1.iterator(partitions(0), context),
      rdd2.iterator(partitions(1), context),
      rdd3.iterator(partitions(2), context)
    )
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    rdd3 = null
    f = null
  }
}

