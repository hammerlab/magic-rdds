package org.hammerlab.magic.rdd

import org.hammerlab.magic.rdd.LazyZippedWithIndexRDD._
import org.hammerlab.magic.util.SparkSuite

class LazyZippedWithIndexRDDTest extends SparkSuite {
  test("match with zipWithIndex") {
    val rdd = sc.parallelize(0 until 10).filter(_ % 2 == 0)
    val res1 = rdd.lazyZipWithIndex.collect
    val res2 = rdd.zipWithIndex.collect
    res1 should be (res2)
  }
}
