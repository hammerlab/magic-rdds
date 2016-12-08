package org.hammerlab.magic.rdd.scan

import org.hammerlab.magic.rdd.scan.ScanRightByKeyRDD._

abstract class ScanRightRDDTestI extends ScanRDDTest {
  import Ops._

  test("strings") {
    check[String](
      "",
      Seq("a", "bc", "", "def"),
      Seq("abcdef", "bcdef", "def", "def")
    )
  }

  test("by-key") {
    val seq =
      Seq(
        "a" → 1,
        "b" → 2,
        "c" → 3,
        "d" → 4,
        "e" → 5
      )

    val actual =
      sc
        .parallelize(seq)
        .scanRightByKey(0)(_ + _)
        .collect()

    actual should be(
      Array(
        "a" → 15,
        "b" → 14,
        "c" → 12,
        "d" →  9,
        "e" →  5
      )
    )
  }
}
