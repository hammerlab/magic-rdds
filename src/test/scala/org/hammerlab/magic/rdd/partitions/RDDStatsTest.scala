package org.hammerlab.magic.rdd.partitions

import org.hammerlab.spark.test.rdd.Util.makeRDD
import org.hammerlab.spark.test.suite.SparkSuite

class RDDStatsTest extends SparkSuite {
  implicit val ordering = implicitly[Ordering[Int]]
  test("sorted") {
    RDDStats(sc.parallelize(0 until 10, 4)) should be(
      SortedRDDStats[Int](
        Array(
          Some(0, 1),
          Some(2, 4),
          Some(5, 6),
          Some(7, 9)
        ),
        Array[Long](2, 3, 2, 3)
      )
    )
  }

  test("first partition unsorted") {
    RDDStats(
      makeRDD(
        List(0, 1, 3, 2),
        4 until 8,
        8 until 12
      )
    ) should be(
      UnsortedRDDStats(
        Array[Long](4, 4, 4)
      )
    )
  }

  test("first partition first element unsorted") {
    RDDStats(
      makeRDD(
        List(1, 0, 3, 2),
        4 until 8,
        8 until 12
      )
    ) should be(
      UnsortedRDDStats(
        Array[Long](4, 4, 4)
      )
    )
  }

  test("last partition last element unsorted") {
    RDDStats(
      makeRDD(
        0 until 4,
        4 until 8,
        List(8, 9, 11, 10)
      )
    ) should be(
      UnsortedRDDStats(
        Array[Long](4, 4, 4)
      )
    )
  }

  test("sorted partitions in unsorted order") {
    RDDStats(
      makeRDD(
        4 until 8,
        0 until 4,
        8 until 12
      )
    ) should be(
      UnsortedRDDStats(
        Array[Long](4, 4, 4)
      )
    )
  }
}
