package org.hammerlab.magic.rdd

import org.hammerlab.magic.test.listener.TestSparkListener
import org.hammerlab.magic.util.SparkSuite
import org.scalatest.BeforeAndAfter

class SampleTest extends SparkSuite with BeforeAndAfter {

  conf.set("spark.extraListeners", "org.hammerlab.magic.test.listener.TestSparkListener")

  def listener = TestSparkListener()
  def numStages = listener.stages.size

  before {
    listener.clear()
  }

  test("sample test") {
    val rdd = sc.parallelize(0 until 10)
    val lazyIndexed = rdd.count
    numStages should be (1)
  }
}
