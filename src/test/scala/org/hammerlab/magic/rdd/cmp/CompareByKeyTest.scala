package org.hammerlab.magic.rdd.cmp

import hammerlab.iterator._
import magic_rdds._
import org.hammerlab.magic.rdd.cmp.Keyed.Stats
import org.hammerlab.spark.test.suite.SparkSuite

import scala.reflect.ClassTag

class CompareByKeyTest
  extends SparkSuite {

  def check[K: ClassTag, V: ClassTag](elems1: Seq[(K, V)],
                                      elems2: Seq[(K, V)],
                                      expected: Stats) = {
    val rdd1 = sc.parallelize(elems1, numSlices = 4)
    val rdd2 = sc.parallelize(elems2, numSlices = 4)

    rdd1.compareByKey(rdd2).stats should be(expected)
  }

  val chars = ('a' to 'l') zipWithIndex

  def slice(start: Int, end: Int) = chars.slice(start, end)

  // Copy of chars, with disjoint values
  val chars2 = chars.mapValues(_ + chars.size)

  def slice2(start: Int, end: Int) = chars2.slice(start, end)

  test("some overlap") {
    check(
      slice(0, 5).reverse ++ slice(0, 10),
      slice(0, 4) ++ slice(8, 12).reverse ++ slice(8, 12).reverse,
      Stats(
        eq = 6,
        extraA = 4,
        extraB = 2,
        onlyA = 5,
        onlyB = 4
      )
    )
  }

  test("equal") {
    check(
      chars,
      chars,
      Stats(eq = 12)
    )
  }

  test("disjoint") {
    check(
      slice(0, 6),
      slice(6, 12),
      Stats(
        onlyA = 6,
        onlyB = 6
      )
    )
  }

  test("mismatched values") {
    check(
      slice(0, 5) ++ slice(0, 10),
      slice(0, 5) ++ slice2(0, 8),
      Stats(
        eq = 5,
        extraA = 5,
        extraB = 0,
        onlyA = 5,
        onlyB = 8
      )
    )
  }
}
