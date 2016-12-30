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

    numJobs should ===(0)

    val indexed = rdd.zipWithIndex

    // only zipWithIndex will trigger job when initialized
    numJobs should ===(1)

    val res1 = lazyIndexed.collect

    numJobs should ===(3)

    val res2 = indexed.collect

    numJobs should ===(4)

    res1 should ===(res2)
  }
}
