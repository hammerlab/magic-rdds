package org.hammerlab.magic.rdd

import org.apache.spark.rdd.RDD
import org.hammerlab.magic.iterator.TakeUntilIterator
import org.hammerlab.magic.rdd.BorrowElemsRDD._

import scala.reflect.ClassTag

class SlidingRDD[T: ClassTag](@transient rdd: RDD[T]) extends Serializable {
  def sliding2(fill: T): RDD[(T, T)] = sliding2(Some(fill))
  def sliding2(fillOpt: Option[T] = None): RDD[(T, T)] =
    rdd
      .copy(1, fillOpt)
      .mapPartitions(
        _
          .sliding(2)
          .withPartial(false)
          .map(l => (l(0), l(1)))
      )

  def sliding3(fill: T): RDD[(T, T, T)] = sliding3(Some(fill))
  def sliding3(fillOpt: Option[T] = None): RDD[(T, T, T)] =
    rdd
      .copy(2, fillOpt)
      .mapPartitions(
        _
          .sliding(3)
          .withPartial(false)
          .map(l => (l(0), l(1), l(2)))
      )


  def sliding(n: Int, fill: T): RDD[Seq[T]] = sliding(n, Some(fill))
  def sliding(n: Int, fillOpt: Option[T] = None): RDD[Seq[T]] =
    rdd
      .copy(n - 1, fillOpt)
      .mapPartitions(
        _
          .sliding(n)
          .withPartial(false)
      )

  def slideUntil(sentinel: T): RDD[Seq[T]] = {
    rdd.shift(it => it.takeWhile(_ != sentinel)).mapPartitions(new TakeUntilIterator(_, sentinel))
  }

}

object SlidingRDD {
  implicit def toSlidingRDD[T: ClassTag](rdd: RDD[T]): SlidingRDD[T] = new SlidingRDD[T](rdd)
}
