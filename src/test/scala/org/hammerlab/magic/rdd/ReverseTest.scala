package org.hammerlab.magic.rdd

import magic_rdds.rev._
import org.hammerlab.magic.rdd.collect.CollectPartitionsRDD._
import org.hammerlab.spark.test.rdd.Util.makeRDD
import org.hammerlab.spark.test.suite.SparkSuite

class ReverseTest extends SparkSuite {
  test("foo") {
    val elems =
      Seq(
        Seq(11, 4, 6),
        Seq(),
        Seq(8, 12, 3, 10, 7, 16, 2),
        Seq(1),
        Seq(17, 15),
        Seq(20, 5, 18, 14),
        Seq(13, 19, 9)
      )

    val rdd = makeRDD(elems: _*)

    rdd.reverse(preservePartitioning = true).collectParts should be(
      elems
        .reverse
        .map(_.reverse)
    )

    rdd.reverse().collectParts should be(
      Seq(
        Seq(9, 19, 13),
        Seq(14, 18, 5),
        Seq(20, 15, 17),
        Seq(1, 2, 16),
        Seq(7, 10, 3),
        Seq(12, 8, 6),
        Seq(4, 11)
      )
    )
  }
}
