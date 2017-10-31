package org.hammerlab.magic.rdd.zip

import magic_rdds.zip._
import org.hammerlab.spark.test.suite.SparkSuite

class ZipPartitionsWithIndexRDDTest extends SparkSuite {
  test("simple") {
    sc.parallelize(1 to 4, numSlices = 4).zipPartitionsWithIndex(sc.parallelize(5 to 8, numSlices = 4))((idx, first, second) ⇒ {
      Iterator(idx → (first.mkString(","), second.mkString(",")))
    }).collect should be(
      Array(
        0 → ("1", "5"),
        1 → ("2", "6"),
        2 → ("3", "7"),
        3 → ("4", "8")
      )
    )
  }
}
