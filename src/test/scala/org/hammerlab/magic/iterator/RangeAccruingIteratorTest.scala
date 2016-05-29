package org.hammerlab.magic.iterator

import org.scalatest.{FunSuite, Matchers}

class RangeAccruingIteratorTest extends FunSuite with Matchers {
  test("empty") {
    new RangeAccruingIterator(Iterator()).toList should be(Nil)
  }

  test("simple") {
    new RangeAccruingIterator(Iterator(2, 4, 5, 7, 8, 9)).toList should be(
      List(
        2 until 3,
        4 until 6,
        7 until 10
      )
    )
  }

  test("one range") {
    new RangeAccruingIterator(Iterator(2, 3, 4, 5, 6, 7, 8, 9)).toList should be(
      List(
        2 until 10
      )
    )
  }

  test("no ranges") {
    new RangeAccruingIterator(Iterator(2, 4, 6, 8)).toList should be(
      List(
        2 until 3,
        4 until 5,
        6 until 7,
        8 until 9
      )
    )
  }
}
