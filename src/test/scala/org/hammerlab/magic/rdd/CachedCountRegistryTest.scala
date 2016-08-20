package org.hammerlab.magic.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.CachedCountRegistry
import org.apache.spark.CachedCountRegistry._
import org.hammerlab.magic.test.listener.TestSparkListener
import org.hammerlab.magic.util.SparkSuite
import org.scalatest.BeforeAndAfter

class CachedCountRegistryTest
  extends SparkSuite
    with BeforeAndAfter {

  conf.set("spark.extraListeners", "org.hammerlab.magic.test.listener.TestSparkListener")

  def listener = TestSparkListener()
  def numStages = listener.stages.size

  before {
    CachedCountRegistry.resetCache()
    listener.clear()
  }

  test("single rdd count") {
    val rdd = sc.parallelize(0 until 4)
    val count = rdd.size()
    count should be (4)
    CachedCountRegistry.getCache should be (Map(rdd.id -> 4))
    numStages should be(1)
  }

  test("multi rdd count") {
    val rdd1 = sc.parallelize(0 until 4)
    val rdd2 = sc.parallelize("a" :: "b" :: Nil)
    val rdd3 = sc.parallelize(Array(true))

    rdd1.size()

    CachedCountRegistry.getCache should be (Map(rdd1.id -> 4))

    numStages should be(1)

    // should apply intermediate cache for 'rdd1'
    val count = (rdd1 :: rdd2 :: rdd3 :: Nil).size()
    count should be (7)
    CachedCountRegistry.getCache should be (
      Map(
        rdd1.id -> 4,
        rdd2.id -> 2,
        rdd3.id -> 1
      )
    )
    numStages should be(2)
  }

  test("union rdds count") {
    val rdd0 = sc.parallelize(0 until 8)
    val rdd1 = sc.parallelize(0 until 4)
    val rdd2 = sc.parallelize(0 until 2)
    val rdd3 = sc.parallelize(0 until 1)

    val rdd01 = rdd0 ++ rdd1
    val rdd23 = rdd2 ++ rdd3

    val rdd01_23 = rdd01 ++ rdd23

    rdd01_23.size() should be(15)
    CachedCountRegistry.getCache should be(
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

    numStages should be(1)

    val rdd02 = rdd0 ++ rdd2
    val rdd13 = rdd1 ++ rdd3

    val rdd02_13 = rdd02 ++ rdd13

    rdd02_13.size() should be(15)
    CachedCountRegistry.getCache should be(
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

    numStages should be(1)
  }

  test("empty multi rdd count") {
    val rdds: List[RDD[Int]] = List.empty[RDD[Int]]
    rdds.size() should be (0)
    CachedCountRegistry.getCache().isEmpty should be (true)
    numStages should be(0)
  }
}
