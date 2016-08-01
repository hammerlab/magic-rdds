package org.hammerlab.magic.stats

import spire.math.Integral

/**
 * Convenience class wrapping a sequence of key-number pairs, used in run-length-encoding in [[Stats]].
 */
case class Runs[K, V: Integral](elems: Seq[(K, V)]) {
  override def toString: String =
    (
      for ((elem, count) <- elems) yield
        if (count == 1)
          elem.toString
        else
          s"$elem×$count"
    ).mkString(", ")
}

object Runs {
  implicit def runsToSeq[K, V: Integral](runs: Runs[K, V]): Seq[(K, V)] = runs.elems
  implicit def seqToRuns[K, V: Integral](elems: Seq[(K, V)]): Runs[K, V] = Runs(elems)
}
