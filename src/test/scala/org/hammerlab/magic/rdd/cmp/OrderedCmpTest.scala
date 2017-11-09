package org.hammerlab.magic.rdd.cmp

import magic_rdds._
import org.hammerlab.magic.rdd.cmp.Ordered.Stats
import org.hammerlab.spark.test.suite.SparkSuite

import scala.reflect.ClassTag

class OrderedCmpTest
  extends SparkSuite {

  def check[T: ClassTag](elems1: T*)(elems2: T*)(expected: Stats) = {
    val rdd1 = sc.parallelize(elems1, numSlices = 4)
    val rdd2 = sc.parallelize(elems2, numSlices = 4)

    rdd1.orderedCmp(rdd2).stats should be(expected)
  }

  test("some overlap") {
    check(
      1 to 10: _*
    )(
      1, 1, 3, 3, 3, 6, 6, 6, 6, 6, 0, 0
    )(
      Stats(
        eq = 3,
        onlyA = 7,
        onlyB = 9
      )
    )
  }

  test("no overlap") {
    check(
      1 to 10: _*
    )(
      (1 to 5) ++ (12 to 7 by -1): _*
    )(
      Stats(
        eq = 6,
        reordered = 3,
        onlyA = 1,
        onlyB = 2
      )
    )
  }

  test("equal") {
    check(
      1 to 10: _*
    )(
      1 to 10: _*
    )(
      Stats(
        eq = 10
      )
    )
  }
}
