package org.hammerlab.magic.rdd.cmp

import hammerlab.monoid._
import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.cmp.Ordered.Stats

import math.min
import scala.reflect.ClassTag

/**
 * Result-container for an ordered comparison of two [[RDD]]s' elements; see [[Stats]]
 */
class Ordered[T: ClassTag] private(joined: RDD[(T, (Iterable[Long], Iterable[Long]))])
  extends Serializable {
  lazy val stats =
    joined
      .values
      .map {
        case (l, r) ⇒
          val (leftIdxs, rightIdxs) = (l.toSet, r.toSet)
          val (numLeft, numRight) = (leftIdxs.size, rightIdxs.size)
          val equal = leftIdxs.intersect(rightIdxs).size
          val common = min(numLeft, numRight)
          Stats(
            eq = equal,
            reordered = common - equal,
            onlyA = numLeft - common,
            onlyB = numRight - common
          )
      }
      .reduce(_ |+| _)

  def isEqual: Boolean = stats.isEqual
}

object Ordered extends Serializable {

  def apply[T: ClassTag](rdd1: RDD[T], rdd2: RDD[T]): Ordered[T] =
    new Ordered[T](
      rdd1
        .zipWithIndex
        .cogroup(
          rdd2.zipWithIndex
        )
    )

  /**
   * Counts of elements
   * @param eq number of indices that have the same element in both RDDs
   * @param reordered number of elements not counted above that nevertheless are equal to an element in the other RDD at
   *                  some other index
   * @param onlyA number of elements only in the left-side RDD
   * @param onlyB number of elements only in the right-side RDD
   */
  case class Stats(eq: Long = 0,
                   reordered: Long = 0,
                   onlyA: Long = 0,
                   onlyB: Long = 0) {
    def isEqual: Boolean =
      reordered == 0 &&
      onlyA == 0 &&
      onlyB == 0
  }
}
