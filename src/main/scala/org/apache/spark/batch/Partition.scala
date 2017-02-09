package org.apache.spark.batch

import org.apache.spark

/**
 * [[Partition]] is a mirror for parent partition of original RDD, and keeps track of partition
 * task, so we can reconstruct partitions on reduce stage.
 */
class Partition(
    val rddId: Long,
    val slice: Int,
    val parent: spark.Partition)
  extends spark.Partition with Serializable {

  override def hashCode: Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: Partition => this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice

  override def toString: String = {
    s"${getClass.getSimpleName}(rddId=$rddId, index=$index, parent=$parent)"
  }
}
