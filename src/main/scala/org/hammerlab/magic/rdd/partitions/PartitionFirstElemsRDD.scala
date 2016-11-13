package org.hammerlab.magic.rdd.partitions

import org.apache.spark.rdd.RDD
import org.hammerlab.spark.util.KeyPartitioner

import scala.collection.immutable.SortedMap
import scala.reflect.ClassTag

/**
 * Helpers for fetching the first element from each partition of an [[RDD]].
 */
class PartitionFirstElemsRDD[T: ClassTag](rdd: RDD[T]) {
  lazy val firstElems: SortedMap[Int, T] = {
    SortedMap(
      rdd.mapPartitionsWithIndex((idx, it) =>
        if (it.hasNext)
          Iterator((idx, it.next()))
        else
          Iterator()
      ).collect(): _*
    )
  }

  def firstElemsMap[U: ClassTag](fn: T => U): scala.collection.Map[Int, U] =
    rdd.mapPartitionsWithIndex((idx, it) =>
      if (it.hasNext)
        Iterator((idx, fn(it.next())))
      else
        Iterator()
    ).collectAsMap()

  def firstElemBounds[U: ClassTag](fn: T => U): IndexedSeq[(Option[U], Option[U])] = {
    val map = firstElemsMap(fn)
    (0 until rdd.getNumPartitions).map(i =>
      (
        map.get(i),
        map.get(i + 1)
      )
    )
  }

  lazy val elemBoundsRDD: RDD[(Option[T], Option[T])] = {
    rdd
      .mapPartitionsWithIndex((idx, it) => {
        if (it.hasNext) {
          val firstElem = it.next()
          if (idx > 0)
            Iterator((idx, false) -> firstElem, (idx - 1, true) -> firstElem)
          else
            Iterator((idx, false) -> firstElem)
        } else
          Iterator()
      })
      .repartitionAndSortWithinPartitions(KeyPartitioner(rdd))
      .mapPartitions(it => {
        var lowerBoundOpt: Option[T] = None
        var upperBoundOpt: Option[T] = None
        for {
          ((idx, isUpper), elem) <- it
        } {
          if (isUpper)
            upperBoundOpt = Some(elem)
          else
            lowerBoundOpt = Some(elem)
        }
        Iterator((lowerBoundOpt, upperBoundOpt))
      })
  }
}

object PartitionFirstElemsRDD {
  implicit def rddToPartitionFirstElemsRDD[T: ClassTag](rdd: RDD[T]): PartitionFirstElemsRDD[T] =
    new PartitionFirstElemsRDD[T](rdd)
}
