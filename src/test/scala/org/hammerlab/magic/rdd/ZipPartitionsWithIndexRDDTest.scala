package org.hammerlab.magic.rdd

import org.hammerlab.magic.util.SparkSuite

import org.apache.spark.zip.ZipPartitionsWithIndexRDD._

class ZipPartitionsWithIndexRDDTest extends SparkSuite {
  test("simple") {
    sc.parallelize(1 to 4, numSlices = 4).zipPartitionsWithIndex(sc.parallelize(5 to 8, numSlices = 4))((idx, first, second) => {
      Iterator(idx -> (first.mkString(","), second.mkString(",")))
    }).collect should be(
      Array(
        0 -> ("1", "5"),
        1 -> ("2", "6"),
        2 -> ("3", "7"),
        3 -> ("4", "8")
      )
    )
  }
}
