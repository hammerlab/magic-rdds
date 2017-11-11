package org.hammerlab.magic.rdd

import magic_rdds._
import org.hammerlab.spark.test.suite.SparkSuite

import Array.fill
import scala.reflect.ClassTag

class SampleTest
  extends SparkSuite {

  def ints(N: Int, n: Int): Seq[(Int, Seq[Int])] = ints[Int](N, n, _ % 2)
  def ints[T: ClassTag](N: Int, n: Int, keyBy: Int ⇒ T): Seq[(T, Seq[Int])] =
    sc
      .parallelize(
        1 to N,
        numSlices = 4
      )
      .keyBy(keyBy)
      .sampleByKey(n)
      .collect
      .toSeq

  test("sample") {
    val Seq((0, evens), (1, odds)) = ints(100, 10)

    evens.size should be(10)
    odds .size should be(10)

    evens map(_ % 2) should be(fill(10)(0))
    odds  map(_ % 2) should be(fill(10)(1))
  }

  test("all") {
    ints(10, 5) should be(
      Seq(
        0 → (2 to 10 by 2),
        1 → (1 to  9 by 2)
      )
    )
  }

  test("oversample") {
    val Seq((false, other), (true, three)) = ints[Boolean](10, 5, _ % 10 == 3)
    three should be(Seq(3))
    other.size should be(5)
    other.exists(_ == 3) should be(false)
  }
}
