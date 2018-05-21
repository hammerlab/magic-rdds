package org.hammerlab.magic.rdd

import magic_rdds._
import org.hammerlab.spark.test.suite.SparkSuite

class CollectTest
  extends SparkSuite {
  implicit def rangeToArray(r: Range): Array[Int] = r.toArray
  test("simple") {
    ==(
      sc
        .parallelize(
          1 to 12,
          numSlices = 4
        )
        .collectParts,
      Array(
         1 to  3,
         4 to  6,
         7 to  9,
        10 to 12
      )
    )
  }
}
