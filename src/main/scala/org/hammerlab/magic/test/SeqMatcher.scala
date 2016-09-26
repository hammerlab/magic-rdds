package org.hammerlab.magic.test

import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.SortedMap
import scala.collection.mutable.ArrayBuffer

/**
 * Custom [[Matcher]] for [[Seq]]s of key-value pairs. Prints nicely-formatted messages about tuples that differ between
 * one collection and another in specific ways:
 *
 *   - keys (and hance values) present in one and not the other.
 *   - keys that are present in both but with differing values.
 *   - pairs that are present in both but in different orders.
 */
case class SeqMatcher[K: Ordering, V](expected: Seq[(K, V)]) extends Matcher[Seq[(K, V)]] {
  override def apply(actual: Seq[(K, V)]): MatchResult = {
    val expectedMap = SortedMap(expected: _*)
    val actualMap = SortedMap(actual: _*)

    val extraElems = actualMap.filterKeys(!expectedMap.contains(_))
    val missingElems = expectedMap.filterKeys(!actualMap.contains(_))

    val diffElems =
      for {
        (k, ev) <- expectedMap
        av <- actualMap.get(k)
        if ev != av
      } yield
        k -> (av, ev)

    val errors = ArrayBuffer[String]()

    errors += "Sequences didn't match!"
    errors += ""

    if (extraElems.nonEmpty) {
      errors += s"Extra elems:"
      errors += extraElems.mkString("\t", "\n\t", "")
      errors += ""
    }

    if (missingElems.nonEmpty) {
      errors += s"Missing elems:"
      errors += missingElems.mkString("\t", "\n\t", "")
      errors += ""
    }

    if (diffElems.nonEmpty) {

      val diffLines =
        for {
          (k, (actualValue, expectedValue)) <- diffElems
        } yield
          s"$k: actual: $actualValue, expected: $expectedValue"

      errors += s"Differing values:"
      errors += diffLines.mkString("\t", "\n\t", "")
      errors += ""
    }

    MatchResult(
      actual == expected,
      errors.mkString("\n"),
      s"$actual matched; was supposed to not."
    )
  }
}

object SeqMatcher {
  def seqMatch[K: Ordering, V](expected: Seq[(K, V)]): Matcher[Seq[(K, V)]] = SeqMatcher[K, V](expected)
  def seqMatch[K: Ordering, V](expected: Array[(K, V)]): Matcher[Seq[(K, V)]] = SeqMatcher[K, V](expected.toList)
}
