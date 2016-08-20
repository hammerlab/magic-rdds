package org.hammerlab.magic.util

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FunSuite, Matchers}

trait SparkSuite
  extends FunSuite
    with SharedSparkContext
    with Matchers {

  implicit lazy val sparkContext = sc

  override def beforeAll(): Unit = {
    conf.set("spark.default.parallelism", "4")
    super.beforeAll()
  }
}
