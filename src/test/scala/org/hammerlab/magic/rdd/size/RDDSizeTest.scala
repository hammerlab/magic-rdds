package org.hammerlab.magic.rdd.size

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.test.NumJobsUtil
import org.hammerlab.spark.test.suite.PerCaseSuite
import org.hammerlab.test.implicits.{convertMap, toSeq, convertTuple2, convertTuple3}

class RDDSizeTest
  extends PerCaseSuite
    with NumJobsUtil {

  implicit val toLongMap = convertMap[Int, Int, Int, Long] _
  implicit val toLongSeq = toSeq[Int, Long] _

  implicit val toLongTuple2 = convertTuple2[Int, Int, Long, Long] _
  implicit val toLongTuple3 = convertTuple3[Int, Int, Int, Long, Long, Long] _

  test("empty") {
    List[RDD[_]]().sizes should be(Nil)
    List[RDD[_]]().total should ===(0)
  }

  test("single rdd count") {
    val rdd = sc.parallelize(0 until 4)
    val size = rdd.size
    size should ===(4)
    getCache should ===(Map(rdd.id → 4))
    numJobs should ===(1)
  }

  test("multiple heterogenous rdds") {
    val rdd1 = sc.parallelize(0 until 4)
    val rdd2 = sc.parallelize("a" :: "b" :: Nil)
    val rdd3 = sc.parallelize(Array(true))

    rdd1.size

    getCache should ===(Map(rdd1.id -> 4))

    numJobs should ===(1)

    // should apply intermediate cache for 'rdd1'
    val rdds = rdd1 :: rdd2 :: rdd3 :: Nil

    rdds.sizes should ===(List(4, 2, 1))
    rdds.total should ===(7)

    getCache ===
      Map(
        rdd1.id -> 4,
        rdd2.id -> 2,
        rdd3.id -> 1
      )

    numJobs should ===(2)
  }

  test("tuple2-RDD sizes") {
    val rdd0 = sc.parallelize(0 until 8)
    val rdd1 = sc.parallelize(0 until 4)
    (rdd0, rdd1).sizes should ===(8, 4)
    (rdd0, rdd1).total should ===(12)
  }

  test("tuple3-RDD sizes") {
    val rdd0 = sc.parallelize(0 until 8)
    val rdd1 = sc.parallelize(0 until 4)
    val rdd2 = sc.parallelize(0 until 2)
    (rdd0, rdd1, rdd2).sizes should ===(8, 4, 2)
    (rdd0, rdd1, rdd2).total should ===(14)
  }

  test("reuse list/union RDDs") {
    val rdd0 = sc.parallelize(0 until 8)
    val rdd1 = sc.parallelize(0 until 4)
    val rdd2 = sc.parallelize(0 until 2)
    val rdd3 = sc.parallelize(0 until 1)

    val rddList = List(rdd0, rdd1, rdd2, rdd3)

    rddList.sizes should ===(List(8, 4, 2, 1))
    getCache ===
      Map(
        rdd0.id -> 8,
        rdd1.id -> 4,
        rdd2.id -> 2,
        rdd3.id -> 1
      )

    numJobs should ===(1)

    val rddList2 = List(rdd0, rdd1, rdd2, rdd3)

    rddList2.sizes should ===(List(8, 4, 2, 1))
    getCache ===
      Map(
        rdd0.id -> 8,
        rdd1.id -> 4,
        rdd2.id -> 2,
        rdd3.id -> 1
      )

    numJobs should ===(1)

    val unionedRDDs = sc.union(rddList)
    unionedRDDs.size should ===(15)
    getCache ===
      Map(
        rdd0.id -> 8,
        rdd1.id -> 4,
        rdd2.id -> 2,
        rdd3.id -> 1,
        unionedRDDs.id -> 15
      )

    numJobs should ===(1)

    val rddList3 = List(rdd0, rdd1, rdd2, rdd3)

    rddList3.sizes should ===(List(8, 4, 2, 1))
    getCache ===
      Map(
        rdd0.id -> 8,
        rdd1.id -> 4,
        rdd2.id -> 2,
        rdd3.id -> 1,
        unionedRDDs.id -> 15
      )

    numJobs should ===(1)
  }

  test("nested union rdds sizes") {
    val rdd0 = sc.parallelize(0 until 8)
    val rdd1 = sc.parallelize(0 until 4)
    val rdd2 = sc.parallelize(0 until 2)
    val rdd3 = sc.parallelize(0 until 1)

    val rdd01 = rdd0 ++ rdd1
    val rdd23 = rdd2 ++ rdd3

    val rdd01_23 = rdd01 ++ rdd23

    rdd01_23.size should ===(15)
    getCache ===
      Map(
        rdd0.id -> 8,
        rdd1.id -> 4,
        rdd2.id -> 2,
        rdd3.id -> 1,
        rdd01.id -> 12,
        rdd23.id -> 3,
        rdd01_23.id -> 15
      )

    numJobs should ===(1)

    val rdd02 = rdd0 ++ rdd2
    val rdd13 = rdd1 ++ rdd3

    val rdd02_13 = rdd02 ++ rdd13

    rdd02_13.size should ===(15)
    getCache ===
      Map(
        rdd0.id -> 8,
        rdd1.id -> 4,
        rdd2.id -> 2,
        rdd3.id -> 1,
        rdd01.id -> 12,
        rdd23.id -> 3,
        rdd01_23.id -> 15,
        rdd02.id -> 10,
        rdd13.id -> 5,
        rdd02_13.id -> 15
      )

    numJobs should ===(1)
  }

  test("empty multi rdd sizes") {
    val rdds: List[RDD[Int]] = Nil
    rdds.sizes should be(Nil)
    getCache.isEmpty should ===(true)
    numJobs should ===(0)
  }
}
