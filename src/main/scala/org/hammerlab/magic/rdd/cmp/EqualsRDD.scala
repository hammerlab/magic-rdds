package org.hammerlab.magic.rdd.cmp

import org.apache.spark.rdd.RDD
import SameElementsRDD._

import scala.reflect.ClassTag

/**
 * Wrap an [[RDD]] and expose `compare`, `compareElements`, and `isEqual` methods for testing its equality to other
 * [[RDD]]s.
 */
class EqualsRDD[T: ClassTag](rdd: RDD[T]) {

  /**
   * Compute statistics about the number of identical elements at identical indices in [[rdd]] vs. another [[RDD]].
   */
  def compare(o: RDD[T]): Cmp[Long, T] = {
    val indexedRDD = rdd.zipWithIndex().map(_.swap)
    val indexedOther = o.zipWithIndex().map(_.swap)

    indexedRDD.compareByKey(indexedOther)
  }

  /**
   * Compare the elements in [[rdd]] to those in another [[RDD]], disregarding the order of each.
   */
  def compareElements(o: RDD[T]): ElemCmp[T] = ElemCmp(rdd, o)

  /**
   * Determine whether [[rdd]] has equal elements in the same order as another [[RDD]].
   */
  def isEqual(o: RDD[T]): Boolean = {
    compare(o).isEqual
  }
}

object EqualsRDD {
  implicit def rddToEqualsRDD[T: ClassTag](rdd: RDD[T]): EqualsRDD[T] = new EqualsRDD(rdd)
}

