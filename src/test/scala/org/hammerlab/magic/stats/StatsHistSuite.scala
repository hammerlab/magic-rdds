package org.hammerlab.magic.stats

import org.scalatest.{FunSuite, Matchers}
import spire.implicits._
import spire.math.Integral

import scala.util.Random

/**
 * Tests of the [[Stats.fromHist]] API for constructing [[Stats]] instances from "histograms" of elements that each come
 * with an associated repetition count, which allows the total number of elements represented to be much larger
 * ([[Long]] vs. [[Int]]).
 */
class StatsHistSuite extends FunSuite with Matchers {

  Random.setSeed(123L)

  def check[V: Integral](input: Seq[(Int, V)], lines: String*): Unit = {
    Stats.fromHist(input).toString should be(lines.mkString("\n"))
  }

  def check[V: Integral](input: Seq[(Int, V)], numToSample: Int, lines: String*): Unit = {
    Stats.fromHist(input, numToSample).toString should be(lines.mkString("\n"))
  }

  def check[V: Integral](input: Seq[(Int, V)], numToSample: Int, onlySampleSorted: Boolean, lines: String*): Unit = {
    Stats.fromHist(input, numToSample, onlySampleSorted).toString should be(lines.mkString("\n"))
  }

  test("empty") {
    check(
      List[(Int, Int)](),
      "(empty)"
    )
  }

  test("single") {
    check(
      List(0 -> 1),
      "num:	1,	mean:	0,	stddev:	0,	mad:	0",
      "elems:	0",
      "50:	0"
    )
  }

  test("double") {
    check(
      List(0 -> 2),
      "num:	2,	mean:	0,	stddev:	0,	mad:	0",
      "elems:	0×2",
      "50:	0"
    )
  }

  test("two singles") {
    check(
      List(0 -> 1, 1 -> 1),
      "num:	2,	mean:	0.5,	stddev:	0.5,	mad:	0.5",
      "elems:	0, 1",
      "50:	0.5"
    )
  }

  test("three singles") {
    check(
      List(0 -> 1, 5 -> 1, 1 -> 1),
      "num:	3,	mean:	2,	stddev:	2.2,	mad:	1",
      "elems:	0, 5, 1",
      "sorted:	0, 1, 5",
      "50:	1"
    )
  }

  test("single double") {
    check(
      List(0 -> 1, 1 -> 2),
      "num:	3,	mean:	0.7,	stddev:	0.5,	mad:	0",
      "elems:	0, 1×2",
      "50:	1"
    )
  }

  test("1×5 2×4") {
    check(
      List(1 -> 5, 2 -> 4),
      "num:	9,	mean:	1.4,	stddev:	0.5,	mad:	0",
      "elems:	1×5, 2×4",
      "25:	1",
      "50:	1",
      "75:	2"
    )
  }

  test("0×5 1×5") {
    check(
      List(0 -> 5, 1 -> 5),
      "num:	10,	mean:	0.5,	stddev:	0.5,	mad:	0.5",
      "elems:	0×5, 1×5",
      "25:	0",
      "50:	0.5",
      "75:	1"
    )
  }

  test("0×4 1×6") {
    check(
      List(0 -> 4, 1 -> 6),
      "num:	10,	mean:	0.6,	stddev:	0.5,	mad:	0",
      "elems:	0×4, 1×6",
      "25:	0",
      "50:	1",
      "75:	1"
    )
  }

  test("x(x) 1 to 10") {
    check(
      (1 to 10).map(i => i -> i),
      "num:	55,	mean:	7,	stddev:	2.4,	mad:	2",
      "elems:	1, 2×2, 3×3, 4×4, 5×5, 6×6, 7×7, 8×8, 9×9, 10×10",
      "5:	2.7",
      "10:	3.4",
      "25:	5",
      "50:	7",
      "75:	9",
      "90:	10",
      "95:	10"
    )
  }

  test("singletons") {
    check(
      (0 to 10).map(i => i -> 1),
      "num:	11,	mean:	5,	stddev:	3.2,	mad:	3",
      "elems:	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10",
      "10:	1",
      "25:	2.5",
      "50:	5",
      "75:	7.5",
      "90:	9"
    )
  }

  test("re-encode") {
    check(
      List(0 -> 1, 0 -> 1, 10 -> 3, 10 -> 4, 3 -> 5, 0 -> 2, 3 -> 1),
      "num:	17,	mean:	5.2,	stddev:	4.2,	mad:	3",
      "elems:	0×2, 10×7, 3×5, 0×2, 3",
      "sorted:	0×4, 3×6, 10×7",
      "10:	0",
      "25:	3",
      "50:	3",
      "75:	10",
      "90:	10"
    )
  }

  test("large hist") {
    check(
      List[(Int, Long)](
        1 -> 10000000000L,
        2 ->   1000000000,
        1 ->          100,
        2 ->   1000000000
      ),
      "num:	12000000100,	mean:	1.2,	stddev:	0.4,	mad:	0",
      "elems:	1×10000000000, 2×1000000000, 1×100, 2×1000000000",
      "sorted:	1×10000000100, 2×2000000000",
      "0.0:	1",
      "0.1:	1",
      "1:	1",
      "5:	1",
      "10:	1",
      "25:	1",
      "50:	1",
      "75:	1",
      "90:	2",
      "95:	2",
      "99:	2",
      "99.9:	2",
      "100.0:	2"
    )
  }
}
