package org.hammerlab.magic.rdd.cmp

import magic_rdds.cmp._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait Equals {
  /**
   * Wrap an [[RDD]] and expose `compare`, `compareElements`, and `isEqual` methods for testing its equality to other
   * [[RDD]]s.
   */
  implicit class EqualsOps[T: ClassTag](rdd: RDD[T]) extends Serializable {

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
}

