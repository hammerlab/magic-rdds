package org.hammerlab.magic.rdd.sliding

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.rdd.RDD
import org.hammerlab.iterator.TakeUntilIterator
import org.hammerlab.magic.rdd.sliding.BorrowElemsRDD._

import scala.reflect.ClassTag

/**
 * Helpers for mimicking Scala collections' "sliding" API on [[RDD]]s; iterates over successive N-element subsequences
 * of the [[RDD]].
 */
class SlidingRDD[T: ClassTag](rdd: RDD[T]) extends Serializable {
  def sliding2(fill: T): RDD[(T, T)] = sliding2(Some(fill))
  def sliding2(fillOpt: Option[T] = None): RDD[(T, T)] =
    rdd
      .copyLeft(1, fillOpt)
      .mapPartitions(
        _
          .sliding(2)
          .withPartial(false)
          .map(l => (l(0), l(1)))
      )

  def sliding3(fill: T): RDD[(T, T, T)] = sliding3(Some(fill))
  def sliding3(fillOpt: Option[T] = None): RDD[(T, T, T)] =
    rdd
      .copyLeft(2, fillOpt)
      .mapPartitions(
        _
          .sliding(3)
          .withPartial(false)
          .map(l => (l(0), l(1), l(2)))
      )


  def sliding(n: Int, fill: T): RDD[Seq[T]] = sliding(n, Some(fill))
  def sliding(n: Int, fillOpt: Option[T] = None): RDD[Seq[T]] =
    rdd
      .copyLeft(n - 1, fillOpt)
      .mapPartitions(
        _
          .sliding(n)
          .withPartial(false)
      )

  def slideUntil(sentinel: T): RDD[Seq[T]] = {
    rdd.shiftLeft(it => it.takeWhile(_ != sentinel)).mapPartitions(new TakeUntilIterator(_, sentinel))
  }

}

object SlidingRDD {
  implicit def toSlidingRDD[T: ClassTag](rdd: RDD[T]): SlidingRDD[T] = new SlidingRDD[T](rdd)

  def register(kryo: Kryo): Unit = {
    // Used in BorrowElemsRDD's partitionOverridesBroadcast.
    kryo.register(classOf[Map[Int, Int]])
    kryo.register(Class.forName("scala.collection.immutable.Map$EmptyMap$"))
  }
}
