package org.hammerlab.magic.test.spark

import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, Suite, TestData}

trait PerCaseSparkContexts
  extends BeforeAndAfterEach {
  self: Suite =>

  implicit var sc: SparkContext = _

  val uuid = new Date().toString + math.floor(math.random * 10E4).toLong.toString

  val appID = s"${this.getClass.getSimpleName}-$uuid"

  val conf =
    new SparkConf()
      //.set("spark.extraListeners", "org.hammerlab.magic.test.listener.TestSparkListener")
      .set("spark.default.parallelism", "4")
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
      .set("spark.ui.enabled", "false")
      .set("spark.app.id", appID)

  override def beforeEach(data: TestData) {
    super.beforeEach(data)
    sc = new SparkContext(conf)
  }

  override def afterEach() {
    super.afterEach()
    sc.stop()

    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")

    sc = null
  }
}
