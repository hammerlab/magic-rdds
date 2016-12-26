package org.hammerlab.magic.rdd.size

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.test.NumJobsUtil
import org.hammerlab.magic.rdd.size.CachedCountRegistry._
import org.hammerlab.spark.test.suite.PerCaseSuite

class CachedCountRegistryTest
  extends PerCaseSuite
    with NumJobsUtil {

  test("empty") {
    List[RDD[_]]().sizes === Nil
    List[RDD[_]]().total === 0
  }

  test("single rdd count") {
    val rdd = sc.parallelize(0 until 4)
    val size = rdd.size
    size === 4
    getCache === Map(sc → rdd.id → 4)
    numJobs === 1
  }

  test("multiple heterogenous rdds") {
    val rdd1 = sc.parallelize(0 until 4)
    val rdd2 = sc.parallelize("a" :: "b" :: Nil)
    val rdd3 = sc.parallelize(Array(true))

    rdd1.size

    getCache === Map(rdd1.id -> 4)

    numJobs === 1

    // should apply intermediate cache for 'rdd1'
    val rdds = rdd1 :: rdd2 :: rdd3 :: Nil

    rdds.sizes === List(4, 2, 1)
    rdds.total === 7

    getCache ===
      Map(
        rdd1.id -> 4,
        rdd2.id -> 2,
        rdd3.id -> 1
      )

    numJobs === 2
  }

  test("tuple2-RDD sizes") {
    val rdd0 = sc.parallelize(0 until 8)
    val rdd1 = sc.parallelize(0 until 4)
    (rdd0, rdd1).sizes === (8, 4)
    (rdd0, rdd1).total === 12
  }

  test("tuple3-RDD sizes") {
    val rdd0 = sc.parallelize(0 until 8)
    val rdd1 = sc.parallelize(0 until 4)
    val rdd2 = sc.parallelize(0 until 2)
    (rdd0, rdd1, rdd2).sizes === (8, 4, 2)
    (rdd0, rdd1, rdd2).total === 14
  }

  test("reuse list/union RDDs") {
    val rdd0 = sc.parallelize(0 until 8)
    val rdd1 = sc.parallelize(0 until 4)
    val rdd2 = sc.parallelize(0 until 2)
    val rdd3 = sc.parallelize(0 until 1)

    val rddList = List(rdd0, rdd1, rdd2, rdd3)

    rddList.sizes === List(8, 4, 2, 1)
    getCache ===
      Map(
        rdd0.id -> 8,
        rdd1.id -> 4,
        rdd2.id -> 2,
        rdd3.id -> 1
      )

    numJobs === 1

    val rddList2 = List(rdd0, rdd1, rdd2, rdd3)

    rddList2.sizes === List(8, 4, 2, 1)
    getCache ===
      Map(
        rdd0.id -> 8,
        rdd1.id -> 4,
        rdd2.id -> 2,
        rdd3.id -> 1
      )

    numJobs === 1

    val unionedRDDs = sc.union(rddList)
    unionedRDDs.size === 15
    getCache ===
      Map(
        rdd0.id -> 8,
        rdd1.id -> 4,
        rdd2.id -> 2,
        rdd3.id -> 1,
        unionedRDDs.id -> 15
      )

    numJobs === 1

    val rddList3 = List(rdd0, rdd1, rdd2, rdd3)

    rddList3.sizes === List(8, 4, 2, 1)
    getCache ===
      Map(
        rdd0.id -> 8,
        rdd1.id -> 4,
        rdd2.id -> 2,
        rdd3.id -> 1,
        unionedRDDs.id -> 15
      )

    numJobs === 1
  }

  test("nested union rdds sizes") {
    val rdd0 = sc.parallelize(0 until 8)
    val rdd1 = sc.parallelize(0 until 4)
    val rdd2 = sc.parallelize(0 until 2)
    val rdd3 = sc.parallelize(0 until 1)

    val rdd01 = rdd0 ++ rdd1
    val rdd23 = rdd2 ++ rdd3

    val rdd01_23 = rdd01 ++ rdd23

    rdd01_23.size === 15
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

    numJobs === 1

    val rdd02 = rdd0 ++ rdd2
    val rdd13 = rdd1 ++ rdd3

    val rdd02_13 = rdd02 ++ rdd13

    rdd02_13.size === 15
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

    numJobs === 1
  }

  test("empty multi rdd sizes") {
    val rdds: List[RDD[Int]] = List.empty[RDD[Int]]
    rdds.sizes === Nil
    getCache.isEmpty === true
    numJobs === 0
  }
}
