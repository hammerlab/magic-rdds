package org.hammerlab.magic.test.listener

import org.hammerlab.magic.test.spark.PerCaseSparkContexts
import org.scalatest.{Suite, TestData}

trait TestListenerSuite extends PerCaseSparkContexts {
  self: Suite =>

  var listener: TestSparkListener = _

  def numStages = listener.stages.size

  conf.set("spark.extraListeners", "org.hammerlab.magic.test.listener.TestSparkListener")

  override def beforeEach(data: TestData) {
    super.beforeEach(data)

    listener = TestSparkListener()
    assert(listener != null)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    listener = null
  }
}
