package org.hammerlab.magic.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class SameElementsRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
  def compareByKey(o: RDD[(K, V)]): Cmp[K, V] = Cmp(rdd.fullOuterJoin(o))
  def sameElements(o: RDD[(K, V)]): Boolean = {
    compareByKey(o).isEqual
  }
}

object SameElementsRDD {
  implicit def rddToSameElementsRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): SameElementsRDD[K, V] = new SameElementsRDD(rdd)
}

case class CmpStats(equal: Long = 0, notEqual: Long = 0, onlyA: Long = 0, onlyB: Long = 0) {
  def +(o: CmpStats): CmpStats =
    CmpStats(
      equal + o.equal,
      notEqual + o.notEqual,
      onlyA + o.onlyA,
      onlyB + o.onlyB
    )

  def isEqual: Boolean =
    notEqual == 0 &&
      onlyA == 0 &&
      onlyB == 0
}

case class Cmp[K: ClassTag, V: ClassTag](joined: RDD[(K, (Option[V], Option[V]))]) {

  lazy val stats =
    (for {
      (idx, (o1, o2)) <- joined
    } yield {
      (o1, o2) match {
        case (Some(e1), Some(e2)) =>
          if (e1 == e2)
            CmpStats(equal = 1)
          else
            CmpStats(notEqual = 1)
        case (Some(e1), _) => CmpStats(onlyA = 1)
        case _ => CmpStats(onlyB = 1)
      }
    }).reduce(_ + _)

  lazy val CmpStats(eq, ne, oa, ob) = stats
  lazy val isEqual = stats.isEqual

  lazy val aOnlyRDD =
    for {
      (k, (aO, bO)) <- joined
      a <- aO
      if bO.isEmpty
    } yield {
      k -> a
    }

  lazy val aOnly = aOnlyRDD.collect()
  def aOnly(num: Int = 10000) = aOnlyRDD.take(num)

  lazy val bOnlyRDD =
    for {
      (k, (aO, bO)) <- joined
      b <- bO
      if aO.isEmpty
    } yield {
      k -> b
    }

  lazy val bOnly = bOnlyRDD.collect()
  def bOnly(num: Int = 10000) = bOnlyRDD.take(num)

  lazy val diffsRDD =
    for {
      (k, (aO, bO)) <- joined
      a <- aO
      b <- bO
      if a != b
    } yield {
      k -> (a, b)
    }

  lazy val diffs = diffsRDD.collect()
  def diffs(num: Int = 10000) = diffsRDD.take(num)

}
