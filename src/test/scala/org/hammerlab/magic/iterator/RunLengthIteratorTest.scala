package org.hammerlab.magic.iterator

import org.scalatest.{FunSuite, Matchers}

class RunLengthIteratorTest extends FunSuite with Matchers {

  def check[T](elems: T*)(expected: (T, Int)*): Unit = {
    RunLengthIterator(elems.iterator).toSeq should be(expected)
  }

  test("empty") {
    check()()
  }

  test("single") {
    check("a")("a" -> 1)
  }

  test("one run") {
    check("a", "a", "a")("a" -> 3)
  }

  test("two singletons") {
    check("a", "b")("a" -> 1, "b" -> 1)
  }

  test("run singleton") {
    check("a", "a", "a", "b")("a" -> 3, "b" -> 1)
  }

  test("singleton run") {
    check("a", "b", "b", "b")("a" -> 1, "b" -> 3)
  }

  test("single run single") {
    check("z", "y", "y", "x")("z" -> 1, "y" -> 2, "x" -> 1)
  }

  test("two runs") {
    check("a", "a", "a", "b", "b", "b", "b")("a" -> 3, "b" -> 4)
  }
}
