package org.hammerlab.magic.util

import scala.util.Random.shuffle
import org.scalatest.{FunSuite, Matchers}

import scala.util.Random

class StatsSuite extends FunSuite with Matchers {

  Random.setSeed(123L)

  def check(input: Seq[Int], lines: String*): Unit = {
    Stats(input).toString should be(lines.mkString("\n"))
  }

  def check(input: Seq[Int], numToSample: Int, lines: String*): Unit = {
    Stats(input, numToSample).toString should be(lines.mkString("\n"))
  }

  test("empty") {
    check(
      Nil,
      "(empty)"
    )
  }

  test("0 to 0") {
    check(
      0 to 0,
      "elems:\t0",
      "sorted:\t0"
    )
  }

  test("0 to 1") {
    check(
      0 to 1,
      "elems:\t0, 1",
      "sorted:\t0, 1"
    )
  }

  test("1 to 0") {
    check(
      1 to 0 by -1,
      "elems:\t1, 0",
      "sorted:\t0, 1"
    )
  }

  test("0 to 2") {
    check(
      0 to 2,
      "elems:\t0, 1, 2",
      "sorted:\t0, 1, 2",
      "50:\t1"
    )
  }

  test("2 to 0") {
    check(
      2 to 0 by -1,
      "elems:\t2, 1, 0",
      "sorted:\t0, 1, 2",
      "50:\t1"
    )
  }

  test("0 to 3") {
    check(
      0 to 3,
      "elems:\t0, 1, 2, 3",
      "sorted:\t0, 1, 2, 3",
      "50:\t1.5"
    )
  }

  test("3 to 0") {
    check(
      3 to 0 by -1,
      "elems:\t3, 2, 1, 0",
      "sorted:\t0, 1, 2, 3",
      "50:\t1.5"
    )
  }

  test("0 to 4") {
    check(
      0 to 4,
      "elems:\t0, 1, 2, 3, 4",
      "sorted:\t0, 1, 2, 3, 4",
      "25:\t1",
      "50:\t2",
      "75:\t3"
    )
  }

  test("4 to 0") {
    check(
      4 to 0 by -1,
      "elems:\t4, 3, 2, 1, 0",
      "sorted:\t0, 1, 2, 3, 4",
      "25:\t1",
      "50:\t2",
      "75:\t3"
    )
  }

  test("0 to 10") {
    check(
      0 to 10,
      "elems:\t0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10",
      "sorted:\t0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10",
      "10:\t1",
      "25:\t2.5",
      "50:\t5",
      "75:\t7.5",
      "90:\t9"
    )
  }

  test("10 to 0") {
    check(
      10 to 0 by -1,
      "elems:\t10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0",
      "sorted:\t0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10",
      "10:\t1",
      "25:\t2.5",
      "50:\t5",
      "75:\t7.5",
      "90:\t9"
    )
  }

  val shuffled0to10 = shuffle(0 to 10).toArray

  test("0 to 10 sample 5") {
    check(
      shuffled0to10,
      numToSample = 5,
      "elems:\t9, 3, 7, 1, 6, …, 4, 8, 2, 0, 10",
      "sorted:\t0, 1, 2, 3, 4, …, 6, 7, 8, 9, 10",
      "10:\t1",
      "25:\t2.5",
      "50:\t5",
      "75:\t7.5",
      "90:\t9"
    )
  }

  test("0 to 10 sample 4") {
    check(
      shuffled0to10,
      numToSample = 4,
      "elems:\t9, 3, 7, 1, …, 8, 2, 0, 10",
      "sorted:\t0, 1, 2, 3, …, 7, 8, 9, 10",
      "10:\t1",
      "25:\t2.5",
      "50:\t5",
      "75:\t7.5",
      "90:\t9"
    )
  }

  test("0 to 10 sample 3") {
    check(
      shuffled0to10,
      numToSample = 3,
      "elems:\t9, 3, 7, …, 2, 0, 10",
      "sorted:\t0, 1, 2, …, 8, 9, 10",
      "10:\t1",
      "25:\t2.5",
      "50:\t5",
      "75:\t7.5",
      "90:\t9"
    )
  }

  test("0 to 10 sample 2") {
    check(
      shuffled0to10,
      numToSample = 2,
      "elems:\t9, 3, …, 0, 10",
      "sorted:\t0, 1, …, 9, 10",
      "10:\t1",
      "25:\t2.5",
      "50:\t5",
      "75:\t7.5",
      "90:\t9"
    )
  }

  test("0 to 10 sample 1") {
    check(
      shuffled0to10,
      numToSample = 1,
      "elems:\t9, …, 10",
      "sorted:\t0, …, 10",
      "10:\t1",
      "25:\t2.5",
      "50:\t5",
      "75:\t7.5",
      "90:\t9"
    )
  }

  test("0 to 10 sample 0") {
    check(
      shuffled0to10,
      numToSample = 0,
      "10:\t1",
      "25:\t2.5",
      "50:\t5",
      "75:\t7.5",
      "90:\t9"
    )
  }

  test("0 to 100") {
    check(
      0 to 100,
      "elems:\t0, 1, 2, 3, 4, 5, 6, 7, 8, 9, …, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100",
      "sorted:\t0, 1, 2, 3, 4, 5, 6, 7, 8, 9, …, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100",
      "1:\t1",
      "5:\t5",
      "10:\t10",
      "25:\t25",
      "50:\t50",
      "75:\t75",
      "90:\t90",
      "95:\t95",
      "99:\t99"
    )
  }

  test("100 to 0") {
    check(
      100 to 0 by -1,
      "elems:\t100, 99, 98, 97, 96, 95, 94, 93, 92, 91, …, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0",
      "sorted:\t0, 1, 2, 3, 4, 5, 6, 7, 8, 9, …, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100",
      "1:\t1",
      "5:\t5",
      "10:\t10",
      "25:\t25",
      "50:\t50",
      "75:\t75",
      "90:\t90",
      "95:\t95",
      "99:\t99"
    )
  }
}
