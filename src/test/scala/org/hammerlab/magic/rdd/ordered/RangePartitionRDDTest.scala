package org.hammerlab.magic.rdd.ordered

import magic_rdds.collect._
import magic_rdds.ordered._
import org.hammerlab.cmp.CanEq
import org.hammerlab.spark.test.rdd.Util.makeRDD
import org.hammerlab.spark.test.suite.SparkSuite

class RangePartitionRDDTest
  extends SparkSuite {

  import scala.collection.immutable.Seq
  import scala.collection.immutable.IndexedSeq
  implicit val arrayCanEqualSeq = CanEq.by[Array[Int], Seq[Int]](_.toSeq.toIndexedSeq)
  implicit val arrayCanEqualIndexedSeq = CanEq.by[Array[Int], IndexedSeq[Int]](_.toSeq.toIndexedSeq)

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

      ==(
        bounds.partitions,
        Array(
          Some( 1, Some( 6)),
          Some( 6, Some(20)),
          Some(20, Some(30)),
          Some(30, Some(40)),
          Some(40, None)
        )
      )

      ==(
        repartitioned.collectPartitions,
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

      ==(
        bounds.partitions,
        Array(
          Some(1, Some(11)),
          Some(11, Some(41)),
          Some(41, None)
        )
      )

      ==(
        repartitioned.collectPartitions,
        Array(
          (1 to 5) ++ (6 to 10 by 2),
          Seq(12, 14) ++ (20 to 25) ++ (30 to 33) ++ Seq(40),
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

      ==(
        bounds.partitions,
        Array(
          None,
          Some( 1, Some( 5)),
          Some( 5, Some( 7)),
          None,
          Some( 7, Some(20)),
          None,
          None,
          Some(20, Some(30)),
          Some(30, Some(40)),
          Some(40, None),
          None
        )
      )

      ==(
        repartitioned.collectPartitions,
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

      ==(
        bounds.toArray,
        Array(
          None,
          Some(10, Some(25)),
          None,
          None,
          Some(25, Some(70)),
          Some(70, None)
        )
      )

      ==(
        repartitioned.collectPartitions,
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
