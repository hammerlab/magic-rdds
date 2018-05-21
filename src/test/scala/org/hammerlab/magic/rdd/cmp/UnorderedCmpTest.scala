package org.hammerlab.magic.rdd.cmp

import magic_rdds._
import org.hammerlab.magic.rdd.cmp.Unordered.Stats
import org.hammerlab.spark.test.suite.SparkSuite

import scala.reflect.ClassTag

class UnorderedCmpTest
  extends SparkSuite {
  def check[T: ClassTag](elems1: T*)(elems2: T*)(expected: Stats) = {
    val rdd1 = sc.parallelize(elems1, numSlices = 4)
    val rdd2 = sc.parallelize(elems2, numSlices = 4)

    ==(rdd1.unorderedCmp(rdd2).stats, expected)
  }

  test("some alignment") {
    check(
      1 to 10: _*
    )(
      1, 1, 3, 3, 3, 6, 6, 6, 6, 6, 0, 0
    )(
      Stats(
        both = 3,
        onlyA = 7,
        onlyB = 9
      )
    )
  }

  test("no alignment") {
    check(
      1 to 10: _*
    )(
      2 to 10: _*
    )(
      Stats(
        both = 9,
        onlyA = 1
      )
    )
  }

  test("all aligned") {
    check(
      1 to 10: _*
    )(
      1 to 10: _*
    )(
      Stats(
        both = 10
      )
    )
  }

}
