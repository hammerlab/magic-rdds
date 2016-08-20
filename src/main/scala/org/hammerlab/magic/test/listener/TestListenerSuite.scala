package org.hammerlab.magic.test.listener

import org.apache.spark.SparkConf
import org.hammerlab.magic.test.spark.PerCaseSparkContexts
import org.scalatest.Suite

trait TestListenerSuite extends PerCaseSparkContexts {
  self: Suite =>

  var listener: TestSparkListener = _

  def numStages = listener.stages.size

  override protected def setConfigs(conf: SparkConf): Unit = {
    conf.set("spark.extraListeners", "org.hammerlab.magic.test.listener.TestSparkListener")
  }

  override def beforeEach() {
    super.beforeEach()

    listener = TestSparkListener()
    assert(listener != null)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    listener = null
  }
}
