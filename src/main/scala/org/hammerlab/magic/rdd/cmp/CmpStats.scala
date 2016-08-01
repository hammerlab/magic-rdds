package org.hammerlab.magic.rdd.cmp

import org.apache.spark.rdd.RDD

/**
 * Summable data-type for counting the number of elements that are the same vs. different in two [[RDD]]s, or that exist
 * in just one or the other.
 */
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
