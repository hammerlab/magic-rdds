package org.hammerlab.parallel

import org.hammerlab.test.Suite
import org.scalatest.Matchers

trait ParallelizerTest
  extends Suite
    with Matchers {

  def make(arr: Array[Int]): Array[String]

  def check(arr: Array[Int]): Unit = {
    make(arr) should be(arr.map(_.toString))
  }

  test("empty") {
    check(Array())
  }

  test("one elem") {
    check(Array(1))
  }

  test("two elems") {
    check(Array(1, 2))
  }

  test("ten elems") {
    check(1 to 10 toArray)
  }
}
