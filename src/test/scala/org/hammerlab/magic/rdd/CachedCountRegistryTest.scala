package org.hammerlab.magic.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.CachedCountRegistry
import org.apache.spark.CachedCountRegistry._

import org.hammerlab.magic.util.SparkSuite

class CachedCountRegistryTest extends SparkSuite {
  test("single rdd count") {
    CachedCountRegistry.resetCache()

    val rdd = sc.parallelize(0 until 4)
    val count = rdd.size()
    count should be (4)
    CachedCountRegistry.getCache should be (Map(rdd.id -> 4))
  }

  test("multi rdd count") {
    CachedCountRegistry.resetCache()

    val rdd1 = sc.parallelize(0 until 4)
    val rdd2 = sc.parallelize("a" :: "b" :: Nil)
    val rdd3 = sc.parallelize(Array(true))

    rdd1.size()
    // index has shifted in previous test, so this RDD.id = 1
    CachedCountRegistry.getCache should be (Map(rdd1.id -> 4))
    // should apply intermediate cache for 'rdd1'
    val count = (rdd1 :: rdd2 :: rdd3 :: Nil).size()
    count should be (7)
    CachedCountRegistry.getCache should be (Map(rdd1.id -> 4, rdd2.id -> 2, rdd3.id -> 1))
  }

  test("empty multi rdd count") {
    CachedCountRegistry.resetCache()

    val rdds: List[RDD[Int]] = List.empty[RDD[Int]]
    rdds.size() should be (0)
    CachedCountRegistry.getCache.isEmpty should be (true)
  }
}
