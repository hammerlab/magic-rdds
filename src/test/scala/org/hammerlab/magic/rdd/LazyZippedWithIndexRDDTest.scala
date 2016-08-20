package org.hammerlab.magic.rdd

import org.hammerlab.magic.rdd.LazyZippedWithIndexRDD._
import org.hammerlab.magic.test.listener.TestSparkListener
import org.hammerlab.magic.util.SparkSuite
import org.scalatest.BeforeAndAfter

class LazyZippedWithIndexRDDTest extends SparkSuite with BeforeAndAfter {

  conf.set("spark.extraListeners", "org.hammerlab.magic.test.listener.TestSparkListener")

  def listener = TestSparkListener()
  def numStages = listener.stages.size

  before {
    listener.clear()
  }

  test("initialize lazy zipWithIndex") {
    val rdd = sc.parallelize(0 until 10)
    val lazyIndexed = rdd.lazyZipWithIndex
    numStages should be (0)
  }

  test("match with zipWithIndex") {
    val rdd = sc.parallelize(0 until 10).filter(_ % 2 == 0)

    val lazyIndexed = rdd.lazyZipWithIndex
    val indexed = rdd.zipWithIndex

    // only zipWithIndex will trigger job when initialized
    numStages should be (1)

    val res1 = lazyIndexed.collect
    val res2 = indexed.collect
    // 2 stages for each RDD (computing count for partitions and indexing each element)
    // note that 1 stage is run for 'zipWithIndex' and 2 stages - for 'lazyZipWithIndex'
    numStages should be (4)

    res1 should be (res2)
  }
}
