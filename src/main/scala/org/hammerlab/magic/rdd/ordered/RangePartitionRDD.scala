package org.hammerlab.magic.rdd.ordered

import org.apache.spark.rdd.RDD
import org.apache.spark.{ NarrowDependency, Partition, TaskContext }
import org.hammerlab.magic.rdd.ordered.SortedRDD.Bounds
import org.hammerlab.spark.{ NumPartitions, PartitionIndex }

import scala.reflect.ClassTag

case class RangePartition(index: PartitionIndex,
                          parents: Seq[Partition])
  extends Partition

case class RangePartitionRDD[T: Ordering: ClassTag](parentRDD: RDD[T],
                                                    partitionParentsMap: IndexedSeq[Option[Seq[PartitionIndex]]],
                                                    bounds: Bounds[T])
  extends RDD[T](
    parentRDD.sparkContext,
    new NarrowDependency[T](parentRDD) {
      override def getParents(partitionId: Int): Seq[Int] =
        partitionParentsMap(partitionId).getOrElse(Nil)
    } :: Nil
  )
    with SortedRDD[T] {

  override def rdd: RDD[T] = this
  override val ord = implicitly[Ordering[T]]

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val RangePartition(index, parents) = split.asInstanceOf[RangePartition]
    bounds(index) match {
      case Some((start, end)) ⇒
        parents
          .iterator
          .flatMap {
            parent ⇒
              parentRDD
              .iterator(
                parent,
                context
              )
              .filter(
                t ⇒
                  ord.lteq(start, t) &&
                    end.forall(ord.lt(t, _))
              )
          }
      case None ⇒
        Iterator()
    }
  }

  private lazy val parentPartitions = parentRDD.partitions

  override protected lazy val getPartitions: Array[Partition] =
    (0 until bounds.numPartitions)
      .map(
        idx ⇒
          RangePartition(
            idx,
            partitionParentsMap(idx)
              .getOrElse(Nil)
              .map(parentPartitions(_))
          )
      )
      .toArray
}
