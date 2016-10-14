package org.hammerlab.magic.rdd.keyed

import org.hammerlab.magic.rdd.keyed.CappedGroupByKeyRDD._
import org.hammerlab.magic.test.rdd.Util.makeRDD
import org.hammerlab.magic.test.spark.SparkSuite

class CappedGroupByKeyRDDTest extends SparkSuite {
  // 4 partitions, each with 20 random ints from [0, 20).
  lazy val rdd =
    makeRDD(
      Vector( 9, 12, 15,  7,  5, 11, 11, 16, 17,  9, 14,  0,  2, 12,  1,  5,  5,  0, 16, 12),
      Vector(13,  2, 19, 10, 18, 16,  7, 17,  3, 13, 11,  5,  0,  1, 18,  9, 11, 10, 11,  3),
      Vector( 4, 17,  1,  5,  8,  4, 14, 13,  2,  1, 11,  8,  1,  1, 13, 15,  7, 14,  1, 14),
      Vector( 0,  2, 15,  2, 16, 10, 17, 19,  6,  4,  1,  7,  8, 14,  5,  7, 16, 16, 18, 12)
    )
    .keyBy(_ % 4)

  test("take 1") {
    rdd.cappedGroupByKey(1).collect() should be(
      List(
        0 -> Vector(12),
        1 -> Vector( 9),
        2 -> Vector(14),
        3 -> Vector(15)
      )
    )
  }

  test("take 2") {
    rdd.cappedGroupByKey(2).collect() should be(
      List(
        0 -> Vector(12, 16),
        1 -> Vector( 9,  5),
        2 -> Vector(14,  2),
        3 -> Vector(15,  7)
      )
    )
  }

  test("take 3") {
    rdd.cappedGroupByKey(3).collect() should be(
      List(
        0 -> Vector(12, 16,  0),
        1 -> Vector( 9,  5, 17),
        2 -> Vector(14,  2,  2),
        3 -> Vector(15,  7, 11)
      )
    )
  }

  test("take 4") {
    rdd.cappedGroupByKey(4).collect() should be(
      List(
        0 -> Vector(12, 16,  0, 12),
        1 -> Vector( 9,  5, 17,  9),
        2 -> Vector(14,  2,  2, 10),
        3 -> Vector(15,  7, 11, 11)
      )
    )
  }

  test("take 5") {
    rdd.cappedGroupByKey(5).collect() should be(
      List(
        0 -> Vector(12, 16,  0, 12,  0),
        1 -> Vector( 9,  5, 17,  9,  1),
        2 -> Vector(14,  2,  2, 10, 18),
        3 -> Vector(15,  7, 11, 11, 19)
      )
    )
  }

  test("take 10") {
    rdd.cappedGroupByKey(10).collect() should be(
      List(
        0 -> Vector(12, 16,  0, 12,  0, 16, 12, 16,  0,  4),
        1 -> Vector( 9,  5, 17,  9,  1,  5,  5, 13, 17, 13),
        2 -> Vector(14,  2,  2, 10, 18, 18, 10, 14,  2, 14),
        3 -> Vector(15,  7, 11, 11, 19,  7,  3, 11, 11, 11)
      )
    )
  }

  test("take 20") {
    rdd.cappedGroupByKey(20).collect() should be(
      List(
        0 -> Vector(12, 16,  0, 12,  0, 16, 12, 16,  0,  4,  8,  4,  8,  0, 16,  4,  8, 16, 16, 12),  // 20 of 20
        1 -> Vector( 9,  5, 17,  9,  1,  5,  5, 13, 17, 13,  5,  1,  9, 17,  1,  5, 13,  1,  1,  1),  // 20 of 25
        2 -> Vector(14,  2,  2, 10, 18, 18, 10, 14,  2, 14, 14,  2,  2, 10,  6, 14, 18),              // 17 of 20
        3 -> Vector(15,  7, 11, 11, 19,  7,  3, 11, 11, 11,  3, 11, 15,  7, 15, 19,  7, 7)            // 18 of 20
      )
    )
  }

  test("take 30") {
    rdd.cappedGroupByKey(30).collect() should be(
      List(
        0 -> Vector(12, 16,  0, 12,  0, 16, 12, 16,  0,  4,  8,  4,  8,  0, 16,  4,  8, 16, 16, 12),                      // 20
        1 -> Vector( 9,  5, 17,  9,  1,  5,  5, 13, 17, 13,  5,  1,  9, 17,  1,  5, 13,  1,  1,  1, 13,  1, 17,  1,  5),  // 25
        2 -> Vector(14,  2,  2, 10, 18, 18, 10, 14,  2, 14, 14,  2,  2, 10,  6, 14, 18),                                  // 17
        3 -> Vector(15,  7, 11, 11, 19,  7,  3, 11, 11, 11,  3, 11, 15,  7, 15, 19,  7, 7)                                // 18
      )
    )
  }
}
