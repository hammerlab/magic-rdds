package org.hammerlab.magic.rdd.cmp

import hammerlab.monoid._
import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.cmp.Unordered.Stats

import scala.reflect.ClassTag
import math.{max, min}

/**
 * Given an outer join of two [[RDD]]s with the presence or absence of values for a key replaced with a [[Boolean]] for
 * each [[RDD]], expose statistics about how many elements exist in either or both [[RDD]]s.
 */
class Unordered[T: ClassTag] private(joined: RDD[(T, (Long, Long))])
  extends Serializable {
  lazy val stats =
    joined
      .map {
        case (e, (a, b)) ⇒
          Stats(
            both = min(a, b),
            onlyA = max(0, a - b),
            onlyB = max(0, b - a)
          )
      }
      .reduce(_ |+| _)


  lazy val Stats(eq, oa, ob) = stats
  lazy val isEqual = stats.isEqual

  lazy val aOnly =
    joined.collect {
      case (e, (a, b))
        if a > b ⇒
        e → (a - b)
    }

  lazy val bOnly =
    joined.collect {
      case (e, (a, b))
        if b > a ⇒
        e → (b - a)
    }
}


object Unordered extends Serializable {
  def apply[T: ClassTag](rdd1: RDD[T], rdd2: RDD[T]): Unordered[T] = {
    val keyedRDD1 = rdd1.map(_ → (1L, 0L))
    val keyedRDD2 = rdd2.map(_ → (0L, 1L))

    val counts = keyedRDD1 ++ keyedRDD2 reduceByKey(_ |+| _)

    new Unordered(counts)
  }

  /**
   * Counts of elements present in either or both of two [[RDD]]s
   */
  case class Stats(both: Long = 0, onlyA: Long = 0, onlyB: Long = 0) {
    def isEqual: Boolean =
      onlyA == 0 &&
        onlyB == 0
  }
}
