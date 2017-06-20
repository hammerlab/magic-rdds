package org.hammerlab.magic.accumulables

import org.apache.spark.AccumulableParam
import spire.implicits._
import spire.math.Numeric

/**
 * Allows for assembling a histogram using a Spark Accumulable.
 *
 * When we put (k, v2) into an accumulator that already contains (k, v1), the result will be a HashMap containing
 * (k, v1 + v2).
 */
class HistogramParam[T, N: Numeric] extends AccumulableParam[HashMap[T, N], T] {

  override def addAccumulator(r: HashMap[T, N], t: T): HashMap[T, N] = {
    if (r.contains(t))
      r(t) += 1
    else
      r(t) = Numeric[N].one
    r
  }

  /**
   * Combine two accumulators. Adds the values in each hash map.
   *
   * This method should modify and return the first value.
   */
  override def addInPlace(first: HashMap[T, N], second: HashMap[T, N]): HashMap[T, N] = {
    for {
      (k, v) ← second
    } {
      if (first.contains(k))
        first(k) += v
      else
        first(k) = v
    }
    first
  }

  /**
   * Zero value for the accumulator: the empty hash map.
   */
  override def zero(initialValue: HashMap[T, N]): HashMap[T, N] = {
    HashMap[T, N]()
  }
}
