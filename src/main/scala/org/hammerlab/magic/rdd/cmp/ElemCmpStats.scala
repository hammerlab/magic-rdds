package org.hammerlab.magic.rdd.cmp

import org.apache.spark.rdd.RDD

/**
 * Summable data type recording the number of elements that are present in either or both of two [[RDD]]s.
 */
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
