package org.hammerlab.magic.rdd.zip

import org.apache.spark.rdd.RDD
import org.apache.spark.zip.ZippedWithIndexRDDPartition
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

/**
 * Slight modification of [[org.apache.spark.rdd.ZippedWithIndexRDD]] from Spark that does not run job when created,
 * it only triggers job on action or computing partitions.
 */
class LazyZippedWithIndexRDD[T: ClassTag](private var rdd: RDD[T]) extends RDD[(T, Long)](rdd) {
  private var startIndices: Array[Long] = _

  val iteratorSize: Iterator[_] ⇒ Long = iter ⇒ iter.size

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
    firstParent[T].partitions.map { x ⇒
      new ZippedWithIndexRDDPartition(x, startIndices(x.index))
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].preferredLocations(split.asInstanceOf[ZippedWithIndexRDDPartition].prev)

  override def compute(splitIn: Partition, context: TaskContext): Iterator[(T, Long)] = {
    val split = splitIn.asInstanceOf[ZippedWithIndexRDDPartition]
    firstParent[T].iterator(split.prev, context).zipWithIndex.map { x ⇒
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
    def lazyZipWithIndex: LazyZippedWithIndexRDD[T] = {
      new LazyZippedWithIndexRDD(rdd)
    }
  }
}
