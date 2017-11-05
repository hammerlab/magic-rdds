package org.hammerlab.magic.rdd.ordered

import magic_rdds.partitions._
import org.apache.spark.rdd.RDD
import hammerlab.iterator.sliding._
import org.hammerlab.magic.rdd.ordered.SortedRDD.Bounds
import org.hammerlab.spark.NumPartitions

import scala.reflect.ClassTag

trait SortedRDD[T] {
  def rdd: RDD[T]
  implicit def ord: Ordering[T]
  def bounds: Bounds[T]
}

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

