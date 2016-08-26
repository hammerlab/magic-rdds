package org.hammerlab.magic.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.test.NumJobsUtil
import org.hammerlab.magic.rdd.CachedCountRegistry._
import org.hammerlab.magic.test.spark.SparkCasesSuite
import org.scalatest.BeforeAndAfterEach

class CachedCountRegistryTest
  extends SparkCasesSuite
    with NumJobsUtil
    with BeforeAndAfterEach {

  var countCache: CachedCountRegistry = _

  override def beforeEach() {
    super.beforeEach()
    countCache = CachedCountRegistry()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    countCache = null
  }

  test("single rdd count") {
    val rdd = sc.parallelize(0 until 4)
    val count = rdd.size
    count should be (4)
    countCache.getCache should be (Map(rdd.id -> 4))
    numJobs should be(1)
  }

  test("multiple heterogenous rdds") {
    val rdd1 = sc.parallelize(0 until 4)
    val rdd2 = sc.parallelize("a" :: "b" :: Nil)
    val rdd3 = sc.parallelize(Array(true))

    rdd1.size

    countCache.getCache should be (Map(rdd1.id -> 4))

    numJobs should be(1)

    // should apply intermediate cache for 'rdd1'
    val rdds = rdd1 :: rdd2 :: rdd3 :: Nil

    rdds.sizes should be(List(4, 2, 1))
    rdds.total should be(7)

    countCache.getCache should be (
      Map(
        rdd1.id -> 4,
        rdd2.id -> 2,
        rdd3.id -> 1
      )
    )
    numJobs should be(2)
  }

  test("reuse list/union RDDs") {
    val rdd0 = sc.parallelize(0 until 8)
    val rdd1 = sc.parallelize(0 until 4)
    val rdd2 = sc.parallelize(0 until 2)
    val rdd3 = sc.parallelize(0 until 1)

    val rddList = List(rdd0, rdd1, rdd2, rdd3)

    rddList.sizes should be(List(8, 4, 2, 1))
    countCache.getCache should be (
      Map(
        rdd0.id -> 8,
        rdd1.id -> 4,
        rdd2.id -> 2,
        rdd3.id -> 1
      )
    )
    numJobs should be(1)

    val rddList2 = List(rdd0, rdd1, rdd2, rdd3)

    rddList2.sizes should be(List(8, 4, 2, 1))
    countCache.getCache should be (
      Map(
        rdd0.id -> 8,
        rdd1.id -> 4,
        rdd2.id -> 2,
        rdd3.id -> 1
      )
    )
    numJobs should be(1)

    val unionedRDDs = sc.union(rddList)
    unionedRDDs.size should be(15)
    countCache.getCache should be (
      Map(
        rdd0.id -> 8,
        rdd1.id -> 4,
        rdd2.id -> 2,
        rdd3.id -> 1,
        unionedRDDs.id -> 15
      )
    )
    numJobs should be(1)

    val rddList3 = List(rdd0, rdd1, rdd2, rdd3)

    rddList3.sizes should be(List(8, 4, 2, 1))
    countCache.getCache should be (
      Map(
        rdd0.id -> 8,
        rdd1.id -> 4,
        rdd2.id -> 2,
        rdd3.id -> 1,
        unionedRDDs.id -> 15
      )
    )
    numJobs should be(1)
  }

  test("nested union rdds sizes") {
    val rdd0 = sc.parallelize(0 until 8)
    val rdd1 = sc.parallelize(0 until 4)
    val rdd2 = sc.parallelize(0 until 2)
    val rdd3 = sc.parallelize(0 until 1)

    val rdd01 = rdd0 ++ rdd1
    val rdd23 = rdd2 ++ rdd3

    val rdd01_23 = rdd01 ++ rdd23

    rdd01_23.size should be(15)
    countCache.getCache should be(
      Map(
        rdd0.id -> 8,
        rdd1.id -> 4,
        rdd2.id -> 2,
        rdd3.id -> 1,
        rdd01.id -> 12,
        rdd23.id -> 3,
        rdd01_23.id -> 15
      )
    )

    numJobs should be(1)

    val rdd02 = rdd0 ++ rdd2
    val rdd13 = rdd1 ++ rdd3

    val rdd02_13 = rdd02 ++ rdd13

    rdd02_13.size should be(15)
    countCache.getCache should be(
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
    )

    numJobs should be(1)
  }

  test("empty multi rdd sizes") {
    val rdds: List[RDD[Int]] = List.empty[RDD[Int]]
    rdds.sizes should be(Nil)
    countCache.getCache.isEmpty should be (true)
    numJobs should be(0)
  }
}
