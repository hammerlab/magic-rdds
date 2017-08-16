package org.hammerlab.magic.rdd.scan

import cats.Monoid
import cats.implicits.{ catsKernelStdGroupForInt, catsKernelStdMonoidForString }
import org.hammerlab.magic.rdd.scan.ScanRightValuesRDD._
import org.hammerlab.magic.rdd.scan.ScanRightRDD._

import scala.reflect.ClassTag

class ScanRightRDDTest
  extends ScanRDDTest {

  def useRDDReversal: Boolean = false

  test("strings") {
    check(
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
        .scanRightValues(useRDDReversal)
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

  def check[T: ClassTag](input: Iterable[T],
                         expectedOpt: Option[Seq[T]] = None)(
      implicit m: Monoid[T]
  ): Unit = {

    val rdd = sc.parallelize(input.toSeq)

    val actualArr =
      rdd
        .scanRight(useRDDReversal)
        .collect()

    val expectedArr =
      expectedOpt.getOrElse(
        input
          .scanRight(m.empty)(m.combine)
          .dropRight(1)
          .toArray
      )

    actualArr should ===(expectedArr)
  }

}
