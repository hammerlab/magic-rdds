package org.hammerlab.magic.rdd.partitions

import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.spark.test.rdd.Util.makeRDD
import RangePartitionRDD._
import CollectPartitionsRDD._

class RangePartitionRDDTest
  extends SparkSuite {
  test("sorted zip") {
    val rdd1 =
      SortedRDD(
        makeRDD(
          1 to 10,
          11 to 40 by 3,
          41 to 60 by 2
        )
      )

    val rdd2 =
      SortedRDD(
        makeRDD(
          1 to 5,
          6 to 15 by 2,
          20 to 25,
          30 to 33,
          40 to 70 by 3
        )
      )

    {
      val SortedRDD(repartitioned, bounds) = rdd1.sortedRepartition(rdd2)

      bounds.toArray should be(
        Array(
          0 → ( 1, Some( 6)),
          1 → ( 6, Some(20)),
          2 → (20, Some(30)),
          3 → (30, Some(40)),
          4 → (40, None)
        )
      )

      repartitioned.collectPartitions should be(
        Array(
          1 to 5,
          (6 to 10) ++ (11 until 20 by 3),
          20 to 30 by 3,
          32 to 40 by 3,
          41 to 60 by 2
        )
      )
    }

    {
      val SortedRDD(repartitioned, bounds) = rdd2.sortedRepartition(rdd1)

      bounds.toArray should be(
        Array(
          0 → (1, Some(11)),
          1 → (11, Some(41)),
          2 → (41, None)
        )
      )

      repartitioned.collectPartitions should be(
        Array(
          (1 to 5) ++ (6 to 10 by 2),
          Array(12, 14) ++ (20 to 25) ++ (30 to 33) ++ Array(40),
          43 to 70 by 3
        )
      )
    }
  }

  test("empty partitions") {
    val rdd1 =
      SortedRDD(
        makeRDD(
          Nil,
          10 to 20,
          Nil,
          Nil,
          25 to 55 by 3,
          70 to 80
        )
      )

    val rdd2 =
      SortedRDD(
        makeRDD(
          Nil,
          1 to 3,
          5 to 5,
          Nil,
          7 to 15 by 2,
          Nil,
          Nil,
          20 to 25,
          30 to 33,
          40 to 70 by 3,
          Nil
        )
      )

    {
      val SortedRDD(repartitioned, bounds) = rdd1.sortedRepartition(rdd2)

      bounds.toArray should be(
        Array(
          1 → ( 1, Some( 5)),
          2 → ( 5, Some( 7)),
          4 → ( 7, Some(20)),
          7 → (20, Some(30)),
          8 → (30, Some(40)),
          9 → (40, None)
        )
      )

      repartitioned.collectPartitions should be(
        Array(
          Nil,                           //  0
          Nil,                           //  1
          Nil,                           //  2
          Nil,                           //  3
          10 until 20,                   //  4
          Nil,                           //  5
          Nil,                           //  6
          List(20, 25, 28),              //  7
          List(31, 34, 37),              //  8
          (40 to 55 by 3) ++ (70 to 80), //  9
          Nil                            // 10
        )
      )
    }

    {
      val SortedRDD(repartitioned, bounds) = rdd2.sortedRepartition(rdd1)

      bounds.toArray should be(
        Array(
          1 → (10, Some(25)),
          4 → (25, Some(70)),
          5 → (70, None)
        )
      )

      repartitioned.collectPartitions should be(
        Array(
          Nil,                                            // 0
          (11 to 15 by 2) ++ (20 until 25),               // 1
          Nil,                                            // 2
          Nil,                                            // 3
          25 :: (30 to 33).toList ++ (40 until 70 by 3),  // 4
          70 to 70                                        // 5
        )
      )
    }
  }
}
