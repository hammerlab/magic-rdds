package org.hammerlab.magic.iterator

import org.scalatest.{FunSuite, Matchers}

class GroupRunsIteratorTest extends FunSuite with Matchers {

  def check(ints: Int*)(strs: String*): Unit = {
    GroupRunsIterator[Int](
      ints,
      _ % 2 == 0
    ).map(
      _.mkString(",")
    ).toList should be(strs)
  }

  test("end with run") {
    check(
      1, 3, 2, 5, 4, 6, 8
    )(
      "1", "3", "2", "5", "4,6,8"
    )
  }

  test("start with run") {
    check(
      2, 4, 6, 8, 1, 10, 3, 5, 7
    )(
      "2,4,6,8", "1", "10", "3", "5", "7"
    )
  }

  test("one run") {
    check(
      2, 4, 6, 8
    )(
      "2,4,6,8"
    )
  }

  test("empty") {
    check()()
  }

  test("no runs") {
    check(
      1, 3, 5, 7
    )(
      "1", "3", "5", "7"
    )
  }

  test ("true singleton") {
    check(2)("2")
  }

  test ("false singleton") {
    check(3)("3")
  }
}
