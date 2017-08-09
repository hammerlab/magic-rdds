package org.hammerlab.magic.rdd.partitions

import org.apache.spark.{ OneToOneDependency, Partition, TaskContext }
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class AppendEmptyPartitionRDD[T: ClassTag](rdd: RDD[T])
  extends RDD[T](
    rdd.sparkContext,
    Seq(
      new OneToOneDependency(rdd)
    )
  ) {

  /** [[RDD.partitions]] is transient, so we denormalize the number of partitions here */
  val num = rdd.getNumPartitions

  override def compute(split: Partition, context: TaskContext): Iterator[T] =
    if (split.index < num)
      rdd.compute(split, context)
    else
      Iterator()

  override def getPartitions: Array[Partition] =
    rdd.partitions :+
      new Partition {
        override def index: Int = num
      }
}

case class AppendEmptyPartition[T: ClassTag](@transient rdd: RDD[T]) {
  def appendEmptyPartition: AppendEmptyPartitionRDD[T] = AppendEmptyPartitionRDD(rdd)
}

object AppendEmptyPartitionRDD {
  implicit def makeAppendEmptyPartitionRDD[T: ClassTag](rdd: RDD[T]): AppendEmptyPartition[T] =
    AppendEmptyPartition(rdd)
}
