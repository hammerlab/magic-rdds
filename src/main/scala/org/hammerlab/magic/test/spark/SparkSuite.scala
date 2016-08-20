package org.hammerlab.magic.test.spark

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FunSuite, Matchers}

trait SparkSuite extends FunSuite with SharedSparkContext with Matchers {
  implicit lazy val sparkContext = sc

  conf.set("spark.default.parallelism", "4")
}
