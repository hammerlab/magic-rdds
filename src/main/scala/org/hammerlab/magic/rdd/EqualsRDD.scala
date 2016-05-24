package org.hammerlab.magic.rdd

import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.SameElementsRDD._

import scala.reflect.ClassTag

class EqualsRDD[T: ClassTag](rdd: RDD[T]) {
  def compare(o: RDD[T]): Cmp[Long, T] = {
    val indexedRDD = rdd.zipWithIndex().map(_.swap)
    val indexedOther = o.zipWithIndex().map(_.swap)

    indexedRDD.compareByKey(indexedOther)
  }

  def compareElements(o: RDD[T]): ElemCmp[T] = {
    ElemCmp(
      for {
        (e, (aO, bO)) <- rdd.map(_ -> null).fullOuterJoin(o.map(_ -> null))
      } yield {
        e -> (aO.isDefined, bO.isDefined)
      }
    )
  }

  def isEqual(o: RDD[T]): Boolean = {
    compare(o).isEqual
  }
}

object EqualsRDD {
  implicit def rddToEqualsRDD[T: ClassTag](rdd: RDD[T]): EqualsRDD[T] = new EqualsRDD(rdd)
}

case class ElemCmp[T: ClassTag](joined: RDD[(T, (Boolean, Boolean))]) {
  lazy val stats =
    (for {
      (e, (a, b)) <- joined
    } yield
      (a, b) match {
        case (true, true) => ElemCmpStats(both = 1)
        case (true, false) => ElemCmpStats(onlyA = 1)
        case (false, true) => ElemCmpStats(onlyB = 1)
        case (false, false) => throw new Exception(s"Invalid entry: $e")
      }
    ).reduce(_ + _)

  lazy val ElemCmpStats(eq, oa, ob) = stats
  lazy val isEqual = stats.isEqual

  lazy val bothRDD =
    for {
      (e, (a, b)) <- joined
      if a && b
    } yield
      e

  lazy val both = bothRDD.collect()
  def both(num: Int = 10000) = bothRDD.take(num)

  lazy val aRDD =
    for {
      (e, (a, b)) <- joined
      if a && !b
    } yield
      e

  lazy val a = aRDD.collect()
  def a(num: Int = 10000) = aRDD.take(num)

  lazy val bRDD =
    for {
      (e, (a, b)) <- joined
      if !a && b
    } yield
      e

  lazy val b = bRDD.collect()
  def b(num: Int = 10000) = bRDD.take(num)
}

case class ElemCmpStats(both: Long = 0, onlyA: Long = 0, onlyB: Long = 0) {
  def +(o: ElemCmpStats): ElemCmpStats =
    ElemCmpStats(
      both + o.both,
      onlyA + o.onlyA,
      onlyB + o.onlyB
    )

  def isEqual: Boolean =
    onlyA == 0 &&
      onlyB == 0
}
