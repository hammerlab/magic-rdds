package org.hammerlab.magic.rdd.zip

import org.apache.spark.scheduler.test.NumJobsUtil
import org.hammerlab.magic.rdd.zip.LazyZippedWithIndexRDD._
import org.hammerlab.spark.test.suite.PerCaseSuite

class LazyZippedWithIndexRDDTest
  extends PerCaseSuite
    with NumJobsUtil {

  test("match with zipWithIndex") {
    val rdd = sc.parallelize(0 until 10).filter(_ % 2 == 0)

    val lazyIndexed = rdd.lazyZipWithIndex

    numJobs === 0

    val indexed = rdd.zipWithIndex

    // only zipWithIndex will trigger job when initialized
    numJobs === 1

    val res1 = lazyIndexed.collect

    numJobs === 3

    val res2 = indexed.collect

    numJobs === 4

    res1 === res2
  }
}
