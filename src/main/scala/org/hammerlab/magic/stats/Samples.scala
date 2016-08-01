package org.hammerlab.magic.stats

import spire.math.Integral
import spire.implicits._

/**
 * Used by [[Stats]] to wrap some [[Runs]] of elements from the start and end of a dataset.
 * @param n total number of elements in the dataset.
 * @param first [[Runs]] of elements from the start of the dataset.
 * @param numFirst the number of elements represented by the [[Runs]] in [[first]], i.e. the sum of the their values.
 * @param last [[Runs]] of elements from the end of the dataset.
 * @param numLast the number of elements represented by the [[Runs]] in [[last]], i.e. the sum of the their values.
 * @tparam K arbitrary element type
 * @tparam V [[Integral]] type, e.g. [[Int]] or [[Long]].
 */
case class Samples[K, V: Integral](n: V, first: Runs[K, V], numFirst: V, last: Runs[K, V], numLast: V) {
  def isEmpty: Boolean = first.isEmpty
  def nonEmpty: Boolean = first.nonEmpty

  def removeOverlap(num: V, first: Runs[K, V], last: Runs[K, V]): Runs[K, V] = {
    val lastIt = last.iterator.buffered
    var dropped = Integral[V].zero
    Runs(
      first ++ lastIt.dropWhile(t => {
        val (_, count) = t
        val drop = dropped < num
        dropped += count
        drop
      })
    )
  }

  override def toString: String = {
    val numSampled = numFirst + numLast
    val numSkipped = n - numSampled
    if (numSkipped > 0) {
      s"$first, …, $last"
    } else {
      removeOverlap(-numSkipped, first, last).toString
    }
  }
}
