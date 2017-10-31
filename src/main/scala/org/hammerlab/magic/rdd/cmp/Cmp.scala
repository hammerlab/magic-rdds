package org.hammerlab.magic.rdd.cmp

import hammerlab.monoid._
import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.cmp.Cmp.Stats

import scala.reflect.ClassTag

/**
 * Given an outer-join of two [[RDD]]s, expose statistics about how many identical elements at identical positions they
 * have.
 */
class Cmp[K: ClassTag, V: ClassTag] private(joined: RDD[(K, (Option[V], Option[V]))]) {

  lazy val stats =
    (for {
      (idx, (o1, o2)) ← joined
    } yield {
      (o1, o2) match {
        case (Some(e1), Some(e2)) ⇒
          if (e1 == e2)
            Stats(equal = 1)
          else
            Stats(notEqual = 1)
        case (Some(e1), _) ⇒ Stats(onlyA = 1)
        case _ ⇒ Stats(onlyB = 1)
      }
    }).reduce(_ |+| _)

  lazy val Stats(eq, ne, oa, ob) = stats
  lazy val isEqual = stats.isEqual

  lazy val aOnlyRDD =
    for {
      (k, (aO, bO)) ← joined
      a ← aO
      if bO.isEmpty
    } yield {
      k → a
    }

  lazy val aOnly = aOnlyRDD.collect()
  def aOnly(num: Int = 10000) = aOnlyRDD.take(num)

  lazy val bOnlyRDD =
    for {
      (k, (aO, bO)) ← joined
      b ← bO
      if aO.isEmpty
    } yield {
      k → b
    }

  lazy val bOnly = bOnlyRDD.collect()
  def bOnly(num: Int = 10000) = bOnlyRDD.take(num)

  lazy val diffsRDD =
    for {
      (k, (aO, bO)) ← joined
      a ← aO
      b ← bO
      if a != b
    } yield {
      k → (a, b)
    }

  lazy val diffs = diffsRDD.collect()
  def diffs(num: Int = 10000) = diffsRDD.take(num)
}

object Cmp {
  def apply[K: ClassTag, V: ClassTag](rdd1: RDD[(K, V)], rdd2: RDD[(K, V)]): Cmp[K, V] =
    new Cmp(rdd1.fullOuterJoin(rdd2))

  /**
   * Summable data-type for counting the number of elements that are the same vs. different in two [[RDD]]s, or that exist
   * in just one or the other.
   */
  case class Stats(equal: Long = 0, notEqual: Long = 0, onlyA: Long = 0, onlyB: Long = 0) {
    def isEqual: Boolean =
      notEqual == 0 &&
        onlyA == 0 &&
        onlyB == 0
  }
}
