package org.hammerlab.magic.util

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FunSuite, Matchers}

trait SparkSuite extends FunSuite with SharedSparkContext with Matchers
