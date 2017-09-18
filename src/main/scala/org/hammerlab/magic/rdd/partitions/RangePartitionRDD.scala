package org.hammerlab.magic.rdd.partitions

import org.apache.spark.rdd.RDD
import org.apache.spark.{ NarrowDependency, Partition, TaskContext }
import org.hammerlab.magic.rdd.partitions.SortedRDD.Bounds
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

trait SortedRDD[T] {
  def rdd: RDD[T]
  implicit def ord: Ordering[T]
  def bounds: Bounds[T]
}


import org.hammerlab.iterator.sliding.Sliding2Iterator._
import org.hammerlab.magic.rdd.partitions.PartitionFirstElemsRDD._

object SortedRDD {

  case class Bounds[T](partitions: IndexedSeq[Option[(T, Option[T])]]) {
    def numPartitions: NumPartitions = partitions.length
  }

  object Bounds {
    implicit def boundsToMap[T](bounds: Bounds[T]): IndexedSeq[Option[(T, Option[T])]] =
      bounds.partitions
  }

  def unapply[T](sr: SortedRDD[T]): Option[(RDD[T], Bounds[T])] = Some(sr.rdd, sr.bounds)

  def apply[T](originalRDD: RDD[T],
               rddBounds: Bounds[T])(
      implicit o: Ordering[T]
  ): SortedRDD[T] =
    new SortedRDD[T] {
      override implicit def ord: Ordering[T] = o
      override def rdd: RDD[T] = originalRDD
      override def bounds: Bounds[T] = rddBounds
    }

  def bounds[T: ClassTag](rdd: RDD[T]): Bounds[T] =
    Bounds(
      {
        val map =
          rdd
            .firstElems
            .iterator
            .sliding2Opt
            .map {
              case ((idx, first), nextOpt) ⇒
                idx → (first, nextOpt.map(_._2))
            }
            .toMap

        (0 until rdd.getNumPartitions).map(i ⇒ map.get(i))
      }
    )

  def apply[T: Ordering: ClassTag](rdd: RDD[T]): SortedRDD[T] =
    SortedRDD(
      rdd,
      bounds(rdd)
    )
}

import org.hammerlab.iterator.HeadOptionIterator
import org.hammerlab.iterator.bulk.BufferedBulkIterator._

object RangePartitionRDD {
  implicit class RangePartitionRDDOps[T: ClassTag](before: SortedRDD[T]) {

    private implicit val ord = before.ord

    def sortedRepartition(after: SortedRDD[T]): SortedRDD[T] = sortedRepartition(after.bounds)
    def sortedRepartition(newBounds: Bounds[T]): SortedRDD[T] =
      RangePartitionRDD(
        before.rdd,
        {
          val bounds =
            before
              .bounds
              .iterator
              .zipWithIndex
              .map(_.swap)
              .flatMap {
                case (idx, boundOpt) ⇒
                  boundOpt.map {
                    case (start, endOpt) ⇒
                      idx -> (start, endOpt)
                  }
              }
              .buffered

          val ord = before.ord

          newBounds
            .map {
              _.map {
                case (start, endOpt) ⇒
                  bounds
                    .dropwhile {
                      case (_, (_, endOpt)) ⇒
                        endOpt.exists(ord.lteq(_, start))
                    }

                  val endsBefore =
                    bounds
                      .collectwhile {
                        case (parentIdx, (_, parentEndOpt))
                          if endOpt.forall(
                            end ⇒
                              parentEndOpt.exists(
                                ord.lteq(_, end)
                              )
                          ) ⇒
                          parentIdx
                      }
                      .toVector

                  bounds
                    .headOption match {
                      case Some((parentIdx, (parentStart, _)))
                        if endOpt.forall(
                          ord.lteq(parentStart, _)
                        ) ⇒
                        endsBefore :+ parentIdx
                      case _ ⇒
                        endsBefore
                  }
              }
            }
        },
        newBounds
      )

    def sortedPartitionZip[U: ClassTag](after: SortedRDD[T],
                                        fn: (Iterator[T], Iterator[T]) ⇒ Iterator[U]): RDD[U] =
      sortedRepartition(after)
        .rdd
        .zipPartitions(after.rdd)(fn)

  }
}
