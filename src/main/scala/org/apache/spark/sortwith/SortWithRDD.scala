package org.apache.spark.sortwith

import java.util.Comparator

import org.apache.spark.rdd.{RDD, ShuffledRDD}

import scala.reflect.ClassTag

class SortWithRDD[T: ClassTag](@transient rdd: RDD[T]) extends Serializable {

  def sortWithCmp(comparator: Comparator[T],
                  numPartitions: Int = rdd.partitions.length,
                  ascending: Boolean = true): RDD[T] = {
    sortWith(comparator.compare, numPartitions, ascending)
  }

  var nil: T = _

  def sortWith(cmpFn: (T, T) => Int,
               numPartitions: Int = rdd.partitions.length,
               ascending: Boolean = true): RDD[T] = {

    val partitioner = new ElementRangePartitioner[T](numPartitions, rdd, cmpFn, ascending)

    def boolCmpFn(t1: T, t2: T): Boolean =
      if (ascending)
        cmpFn(t1, t2) < 0
      else
        cmpFn(t1, t2) > 0

    new ShuffledRDD[T, T, T](rdd.map(_ -> nil), partitioner).mapPartitions(iter => {
      val arr = iter.map(_._1).toArray
      try {
        arr.sortWith(boolCmpFn).toIterator
      } catch {
        case e: IllegalArgumentException =>
          throw new IllegalArgumentException(
            s"Inconsistent sort:\n\t${arr.take(1000).map({
              case ((t1, t2, t3), i) => s"($t1,$t2,$t3) -> $i"
              case o => o.toString
            }).mkString("\n\t")}",
            e
          )
      }
    })
  }
}

object SortWithRDD {
  implicit def rddToSortWithRdd[T: ClassTag](rdd: RDD[T]): SortWithRDD[T] = new SortWithRDD[T](rdd)
}
