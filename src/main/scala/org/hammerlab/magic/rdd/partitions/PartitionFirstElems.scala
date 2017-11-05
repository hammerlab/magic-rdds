package org.hammerlab.magic.rdd.partitions

import hammerlab.iterator._
import org.apache.spark.rdd.RDD
import org.hammerlab.spark.KeyPartitioner

import scala.collection.immutable.SortedMap
import scala.reflect.ClassTag

/**
 * Helpers for fetching the first element from each partition of an [[RDD]].
 */
trait PartitionFirstElems {
  implicit class PartitionFirstElemsOps[T: ClassTag](rdd: RDD[T]) extends Serializable {
    lazy val firstElems: SortedMap[Int, T] =
      SortedMap(
        rdd
          .mapPartitionsWithIndex(
              (idx, it) ⇒
                if (it.hasNext)
                  Iterator(idx → it.next())
                else
                  Iterator()
          )
          .collect(): _*
      )

    lazy val firstElemsOpt: Array[Option[T]] =
      rdd
        .mapPartitions(
          it ⇒
            Iterator(it.nextOption)
        )
        .collect()

    def firstElemsMap[U: ClassTag](fn: T ⇒ U): scala.collection.Map[Int, U] =
      rdd
        .mapPartitionsWithIndex(
          (idx, it) ⇒
            if (it.hasNext)
              Iterator((idx, fn(it.next())))
            else
              Iterator()
        )
        .collectAsMap()

    def firstElemBounds[U: ClassTag](fn: T ⇒ U): IndexedSeq[(Option[U], Option[U])] = {
      val map = firstElemsMap(fn)
      (0 until rdd.getNumPartitions).map(i ⇒
        (
          map.get(i),
          map.get(i + 1)
        )
      )
    }

    lazy val elemBoundsRDD: RDD[(Option[T], Option[T])] = {
      rdd
        .mapPartitionsWithIndex((idx, it) ⇒
          if (it.hasNext) {
            val firstElem = it.next()
            if (idx > 0)
              Iterator(
                (idx, false) → firstElem,
                (idx - 1, true) → firstElem
              )
            else
              Iterator(
                (idx, false) → firstElem
              )
          } else
            Iterator()
        )
        .repartitionAndSortWithinPartitions(KeyPartitioner(rdd))
        .mapPartitions {
          it ⇒
            var lowerBoundOpt: Option[T] = None
            var upperBoundOpt: Option[T] = None
            for {
              ((idx, isUpper), elem) ← it
            } {
              if (isUpper)
                upperBoundOpt = Some(elem)
              else
                lowerBoundOpt = Some(elem)
            }

            Iterator(
              (
                lowerBoundOpt,
                upperBoundOpt
              )
            )
        }
    }
  }
}
