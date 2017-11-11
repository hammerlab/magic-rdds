package org.hammerlab.magic.rdd

import magic_rdds._
import org.hammerlab.spark.test.suite.SparkSuite

class CollectTest
  extends SparkSuite {
  test("simple") {
    sc.parallelize(1 to 12, numSlices = 4).collectParts should be(
      Array(
         1 to  3,
         4 to  6,
         7 to  9,
        10 to 12
      )
    )
  }
}
