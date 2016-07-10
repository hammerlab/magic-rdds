package org.hammerlab.magic.util

import scala.util.Random.shuffle
import org.scalatest.{FunSuite, Matchers}
import spire.implicits._
import spire.math.{Numeric, Integral}

import scala.util.Random

trait StatsSuiteBase extends FunSuite with Matchers {

}

class StatsSuite extends FunSuite with Matchers {

  Random.setSeed(123L)

  def check(input: Seq[Int], lines: String*): Unit = {
    Stats(input).toString should be(lines.mkString("\n"))
  }

  def check(input: Seq[Int], numToSample: Int, lines: String*): Unit = {
    Stats(input, numToSample).toString should be(lines.mkString("\n"))
  }

  def check(input: Seq[Int], numToSample: Int, onlySampleSorted: Boolean, lines: String*): Unit = {
    Stats(input, numToSample, onlySampleSorted).toString should be(lines.mkString("\n"))
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
      "num:	1,	mean:	0,	stddev:	0,	mad:	0",
      "elems:	0"
    )
  }

  test("0 to 1") {
    check(
      0 to 1,
      "num:	2,	mean:	0.5,	stddev:	0.5,	mad:	0.5",
      "elems:	0, 1"
    )
  }

  test("1 to 0") {
    check(
      1 to 0 by -1,
      "num:	2,	mean:	0.5,	stddev:	0.5,	mad:	0.5",
      "elems:	1, 0",
      "sorted:	0, 1"
    )
  }

  test("0 to 2") {
    check(
      0 to 2,
      "num:	3,	mean:	1,	stddev:	0.8,	mad:	1",
      "elems:	0, 1, 2",
      "50:	1"
    )
  }

  test("2 to 0") {
    check(
      2 to 0 by -1,
      "num:	3,	mean:	1,	stddev:	0.8,	mad:	1",
      "elems:	2, 1, 0",
      "sorted:	0, 1, 2",
      "50:	1"
    )
  }

  test("0 to 3") {
    check(
      0 to 3,
      "num:	4,	mean:	1.5,	stddev:	1.1,	mad:	1",
      "elems:	0, 1, 2, 3",
      "50:	1.5"
    )
  }

  test("3 to 0") {
    check(
      3 to 0 by -1,
      "num:	4,	mean:	1.5,	stddev:	1.1,	mad:	1",
      "elems:	3, 2, 1, 0",
      "sorted:	0, 1, 2, 3",
      "50:	1.5"
    )
  }

  test("0 to 4") {
    check(
      0 to 4,
      "num:	5,	mean:	2,	stddev:	1.4,	mad:	1",
      "elems:	0, 1, 2, 3, 4",
      "25:	1",
      "50:	2",
      "75:	3"
    )
  }

  test("4 to 0") {
    check(
      4 to 0 by -1,
      "num:	5,	mean:	2,	stddev:	1.4,	mad:	1",
      "elems:	4, 3, 2, 1, 0",
      "sorted:	0, 1, 2, 3, 4",
      "25:	1",
      "50:	2",
      "75:	3"
    )
  }

  test("0 to 10") {
    check(
      0 to 10,
      "num:	11,	mean:	5,	stddev:	3.2,	mad:	3",
      "elems:	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10",
      "10:	1",
      "25:	2.5",
      "50:	5",
      "75:	7.5",
      "90:	9"
    )
  }

  test("10 to 0") {
    check(
      10 to 0 by -1,
      "num:	11,	mean:	5,	stddev:	3.2,	mad:	3",
      "elems:	10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0",
      "sorted:	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10",
      "10:	1",
      "25:	2.5",
      "50:	5",
      "75:	7.5",
      "90:	9"
    )
  }

  val shuffled0to10 = shuffle(0 to 10).toArray

  test("0 to 10 sample 5") {
    check(
      shuffled0to10,
      numToSample = 5,
      "num:	11,	mean:	5,	stddev:	3.2,	mad:	3",
      "elems:	9, 3, 7, 1, 6, …, 4, 8, 2, 0, 10",
      "sorted:	0, 1, 2, 3, 4, …, 6, 7, 8, 9, 10",
      "10:	1",
      "25:	2.5",
      "50:	5",
      "75:	7.5",
      "90:	9"
    )
  }

  test("0 to 10 sample 4") {
    check(
      shuffled0to10,
      numToSample = 4,
      "num:	11,	mean:	5,	stddev:	3.2,	mad:	3",
      "elems:	9, 3, 7, 1, …, 8, 2, 0, 10",
      "sorted:	0, 1, 2, 3, …, 7, 8, 9, 10",
      "10:	1",
      "25:	2.5",
      "50:	5",
      "75:	7.5",
      "90:	9"
    )
  }

  test("0 to 10 sample 3") {
    check(
      shuffled0to10,
      numToSample = 3,
      "num:	11,	mean:	5,	stddev:	3.2,	mad:	3",
      "elems:	9, 3, 7, …, 2, 0, 10",
      "sorted:	0, 1, 2, …, 8, 9, 10",
      "10:	1",
      "25:	2.5",
      "50:	5",
      "75:	7.5",
      "90:	9"
    )
  }

  test("0 to 10 sample 2") {
    check(
      shuffled0to10,
      numToSample = 2,
      "num:	11,	mean:	5,	stddev:	3.2,	mad:	3",
      "elems:	9, 3, …, 0, 10",
      "sorted:	0, 1, …, 9, 10",
      "10:	1",
      "25:	2.5",
      "50:	5",
      "75:	7.5",
      "90:	9"
    )
  }

  test("0 to 10 sample 1") {
    check(
      shuffled0to10,
      numToSample = 1,
      "num:	11,	mean:	5,	stddev:	3.2,	mad:	3",
      "elems:	9, …, 10",
      "sorted:	0, …, 10",
      "10:	1",
      "25:	2.5",
      "50:	5",
      "75:	7.5",
      "90:	9"
    )
  }

  test("0 to 10 sample 0") {
    check(
      shuffled0to10,
      numToSample = 0,
      "num:	11,	mean:	5,	stddev:	3.2,	mad:	3",
      "10:	1",
      "25:	2.5",
      "50:	5",
      "75:	7.5",
      "90:	9"
    )
  }

  test("0 to 100") {
    check(
      0 to 100,
      "num:	101,	mean:	50,	stddev:	29.2,	mad:	25",
      "elems:	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, …, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100",
      "1:	1",
      "5:	5",
      "10:	10",
      "25:	25",
      "50:	50",
      "75:	75",
      "90:	90",
      "95:	95",
      "99:	99"
    )
  }

  test("100 to 0") {
    check(
      100 to 0 by -1,
      "num:	101,	mean:	50,	stddev:	29.2,	mad:	25",
      "elems:	100, 99, 98, 97, 96, 95, 94, 93, 92, 91, …, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0",
      "sorted:	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, …, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100",
      "1:	1",
      "5:	5",
      "10:	10",
      "25:	25",
      "50:	50",
      "75:	75",
      "90:	90",
      "95:	95",
      "99:	99"
    )
  }

  val shuffledDigits = (0 until 100).map(_ => Random.nextInt(10))

  test("100 digits") {
    check(
      shuffledDigits,
      "num:	100,	mean:	4.3,	stddev:	2.9,	mad:	2",
      "elems:	9, 6, 2, 5, 7, 9, 0, 5, 4, 6, …, 1, 9, 0×2, 8, 0, 7×2, 0, 6, 2, 4",
      "sorted:	0×15, 1×7, 2×9, 3×10, 4×10, 5×11, 6×11, 7×9, 8×9, 9×9",
      "5:	0",
      "10:	0",
      "25:	2",
      "50:	4",
      "75:	7",
      "90:	8",
      "95:	9"

    )
  }

  test("100 digits sample 4") {
    check(
      shuffledDigits,
      numToSample = 4,
      "num:	100,	mean:	4.3,	stddev:	2.9,	mad:	2",
      "elems:	9, 6, 2, 5, …, 0, 6, 2, 4",
      "sorted:	0×15, 1×7, 2×9, 3×10, …, 6×11, 7×9, 8×9, 9×9",
      "5:	0",
      "10:	0",
      "25:	2",
      "50:	4",
      "75:	7",
      "90:	8",
      "95:	9"
    )
  }

  test("100 digits sample 4 only sample sorted") {
    check(
      shuffledDigits,
      numToSample = 4,
      onlySampleSorted = true,
      "num:	100,	mean:	4.3,	stddev:	2.9,	mad:	2",
      "sorted:	0×15, 1×7, 2×9, 3×10, …, 6×11, 7×9, 8×9, 9×9",
      "5:	0",
      "10:	0",
      "25:	2",
      "50:	4",
      "75:	7",
      "90:	8",
      "95:	9"
    )
  }

  val sortedShuffledDigits = shuffledDigits.sorted

  test("100 sorted digits") {
    check(
      sortedShuffledDigits,
      "num:	100,	mean:	4.3,	stddev:	2.9,	mad:	2",
      "elems:	0×15, 1×7, 2×9, 3×10, 4×10, 5×11, 6×11, 7×9, 8×9, 9×9",
      "5:	0",
      "10:	0",
      "25:	2",
      "50:	4",
      "75:	7",
      "90:	8",
      "95:	9"
    )
  }

  test("100 sorted digits only sample sorted overridden") {
    check(
      sortedShuffledDigits,
      numToSample = 4,
      onlySampleSorted = true,
      "num:	100,	mean:	4.3,	stddev:	2.9,	mad:	2",
      "elems:	0×15, 1×7, 2×9, 3×10, …, 6×11, 7×9, 8×9, 9×9",
      "5:	0",
      "10:	0",
      "25:	2",
      "50:	4",
      "75:	7",
      "90:	8",
      "95:	9"
    )
  }
}

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
