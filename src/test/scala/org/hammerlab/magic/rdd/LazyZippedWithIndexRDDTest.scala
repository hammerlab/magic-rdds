package org.hammerlab.magic.rdd

import org.apache.spark.scheduler.test.ContextUtil
import org.hammerlab.magic.rdd.LazyZippedWithIndexRDD._
import org.hammerlab.magic.test.spark.SparkCasesSuite

class LazyZippedWithIndexRDDTest
  extends SparkCasesSuite
    with ContextUtil {

  test("match with zipWithIndex") {
    val rdd = sc.parallelize(0 until 10).filter(_ % 2 == 0)

    val lazyIndexed = rdd.lazyZipWithIndex

    numJobs should be(0)

    val indexed = rdd.zipWithIndex

    // only zipWithIndex will trigger job when initialized
    numJobs should be(1)

    val res1 = lazyIndexed.collect

    numJobs should be(3)

    val res2 = indexed.collect

    numJobs should be(4)

    res1 should be (res2)
  }
}
