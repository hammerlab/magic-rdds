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
     * Compute statistics about numbers of common elements between two [[RDD]]s, distinguishing those at identical
     * indices to those that aren't.
     */
    def orderedCmp(o: RDD[T]): Ordered[T] = Ordered(rdd, o)

    /**
     * Compare the elements in [[rdd]] to those in another [[RDD]], disregarding order/positions of elements.
     */
    def unorderedCmp(o: RDD[T]): Unordered[T] = Unordered(rdd, o)

    /**
     * Determine whether [[rdd]] has equal elements in the same order as another [[RDD]].
     */
    def isEqual(o: RDD[T]): Boolean = orderedCmp(o).isEqual

    def sameElements(o: RDD[T]): Boolean = unorderedCmp(o).isEqual
  }
}

