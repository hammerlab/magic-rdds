package org.hammerlab.magic.math

import org.hammerlab.magic.iterator.SimpleBufferedIterator
import spire.math.Integral
import spire.implicits._

/**
 * Emit an exponentially-increasing sequence of integers composed of repetitions of `steps` scaled by successive powers
 * of `base`.
 */
class RoundNumbers[I: Integral] private(steps: Seq[Int],
                                               base: Int = 10,
                                               limitOpt: Option[I])
  extends SimpleBufferedIterator[I] {

  private var idx = 0
  private var basePow: I = Integral[I].one

  override protected def _advance: Option[I] = {
    val n = steps(idx) * basePow
    if (limitOpt.exists(_ < n))
      None
    else
      Some(n)
  }

  override protected def postNext(): Unit = {
    idx += 1
    if (idx == steps.size) {
      idx = 0
      basePow *= base
    }
  }
}

/**
 * Constructors.
 */
object RoundNumbers {
  def apply[I: Integral](steps: Seq[Int],
                         limit: I,
                         base: Int = 10): Iterator[I] =
    new RoundNumbers(steps, base, Some(limit))

  def apply(steps: Seq[Int],
            base: Int = 10): Iterator[Long] =
    new RoundNumbers[Long](steps, base, None)
}
