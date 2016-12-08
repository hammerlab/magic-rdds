package org.hammerlab.magic.rdd.scan

import org.hammerlab.magic.rdd.scan.ScanLeftByKeyRDD._
import org.hammerlab.magic.rdd.scan.ScanLeftRDD._

import scala.reflect.ClassTag

class ScanLeftRDDTest extends ScanRDDTest {

  import Ops._

  def check[T: ClassTag](identity: T, input: Iterable[T], op: (T, T) ⇒ T, expectedOpt: Option[Seq[T]] = None): Unit = {
    val actualArr =
      sc
        .parallelize(input.toSeq)
        .scanLeft(identity)(op)
        .collect()

    val expectedArr =
      expectedOpt.getOrElse(
        input
          .scanLeft(identity)(op)
          .drop(1)
          .toArray
      )

    actualArr should be(expectedArr)
  }

  test("strings") {
    check[String](
      "",
      Seq("a", "bc", "", "def"),
      Seq("a", "abc", "abc", "abcdef")
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
        .scanLeftByKey(0)(_ + _)
        .collect()

    actual should be(
      Array(
        "a" →  1,
        "b" →  3,
        "c" →  6,
        "d" → 10,
        "e" → 15
      )
    )
  }
}
