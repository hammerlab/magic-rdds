package org.hammerlab.magic.rdd

import org.apache.spark.rdd.RDD

import scala.collection.immutable.SortedMap
import scala.reflect.ClassTag

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

  //lazy val firstElemOpts: Array[(Int, Option[T])] = firstElemsMap(x => x)

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

  def firstElemBoundsRDD[U: ClassTag](fn: T => U): RDD[(Option[U], Option[U])] =
    rdd.sparkContext.parallelize(firstElemBounds(fn), rdd.getNumPartitions)

}

object PartitionFirstElemsRDD {
  implicit def rddToPartitionFirstElemsRDD[T: ClassTag](rdd: RDD[T]): PartitionFirstElemsRDD[T] =
    new PartitionFirstElemsRDD[T](rdd)
}
