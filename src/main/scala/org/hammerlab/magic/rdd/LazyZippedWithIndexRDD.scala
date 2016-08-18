package org.hammerlab.magic.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD

/** Copy of [[ZippedWithIndexRDDPartition]] that cannot be accessed outside spark package */
class LazyZippedWithIndexRDDPartition(val prev: Partition, val startIndex: Long)
  extends Partition with Serializable {

  override val index: Int = prev.index
}

/**
 * Slight modification of [[ZippedWithIndexRDD]] from Spark that does not run job when created,
 * it only triggers job on action or computing partitions.
 */
class LazyZippedWithIndexRDD[T: ClassTag](private var rdd: RDD[T]) extends RDD[(T, Long)](rdd) {
  private var startIndices: Array[Long] = null

  val iteratorSize: Iterator[_] => Long = iter => {
    var count = 0L
    while (iter.hasNext) {
      count += 1L
      iter.next()
    }
    count
  }

  /** The start index of each partition. */
  def getStartIndices(rdd: RDD[_]): Array[Long] = {
    val n = rdd.partitions.length
    if (n == 0) {
      Array[Long]()
    } else if (n == 1) {
      Array(0L)
    } else {
      rdd.context.runJob(
        rdd,
        iteratorSize,
        0 until n - 1 // do not need to count the last partition
      ).scanLeft(0L)(_ + _)
    }
  }

  override def getPartitions: Array[Partition] = {
    if (startIndices == null) {
      startIndices = getStartIndices(rdd)
    }
    firstParent[T].partitions.map { x =>
      new LazyZippedWithIndexRDDPartition(x, startIndices(x.index))
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].preferredLocations(split.asInstanceOf[LazyZippedWithIndexRDDPartition].prev)

  override def compute(splitIn: Partition, context: TaskContext): Iterator[(T, Long)] = {
    val split = splitIn.asInstanceOf[LazyZippedWithIndexRDDPartition]
    firstParent[T].iterator(split.prev, context).zipWithIndex.map { x =>
      (x._1, split.startIndex + x._2)
    }
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd = null
  }
}

object LazyZippedWithIndexRDD {
  implicit class ImplicitZipWithIndex[T: ClassTag](rdd: RDD[T]) {
    def lazyZipWithIndex(): LazyZippedWithIndexRDD[T] = {
      new LazyZippedWithIndexRDD(rdd)
    }
  }
}
