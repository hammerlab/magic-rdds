package org.hammerlab.magic.rdd.sliding

import org.hammerlab.magic.rdd.sliding.ScanLeftRDD._
import org.hammerlab.spark.test.suite.SparkSuite

class ScanLeftRDDTest extends SparkSuite {

  def check(seq: Iterable[Int]): Unit = {
    val actual =
      sc
        .parallelize(seq.toSeq)
        .scanLeft(0)(_ + _)
        .collect()

    val expected =
      seq
        .scanLeft(0)(_ + _)
        .drop(1)
        .toArray

    actual should be(expected)
  }

  test("0") { check(Nil) }
  test("1") { check(1 to 1) }
  test("2") { check(1 to 2) }
  test("3") { check(1 to 3) }
  test("4") { check(1 to 4) }
  test("5") { check(1 to 5) }
  test("6") { check(1 to 6) }
  test("7") { check(1 to 7) }
  test("8") { check(1 to 8) }
  test("9") { check(1 to 9) }
  test("10") { check(1 to 10) }

  test("strings") {
    val seq = Seq("a", "bc", "", "def")
    val actual =
      sc
      .parallelize(seq)
      .scanLeft("")(_ + _)
      .collect()

    actual should be(Array("a", "abc", "abc", "abcdef"))
  }
}
