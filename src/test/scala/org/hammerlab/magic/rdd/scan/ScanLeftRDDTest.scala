package org.hammerlab.magic.rdd.scan

import cats.implicits.{ catsKernelStdGroupForInt, catsKernelStdMonoidForString }
import cats.kernel.Monoid
import org.hammerlab.magic.rdd.scan.ScanLeftValuesRDD._
import org.hammerlab.magic.rdd.scan.ScanLeftRDD._

import scala.reflect.ClassTag

class ScanLeftRDDTest
  extends ScanRDDTest {

  override def check[T: ClassTag](input: Iterable[T],
                                  expectedOpt: Option[Seq[T]] = None)(
      implicit
      m: Monoid[T]
  ): Unit = {
    val actualArr =
      sc
        .parallelize(input.toSeq)
        .scanLeft
        .collect()

    val expectedArr =
      expectedOpt.getOrElse(
        input
          .scanLeft(m.empty)(m.combine)
          .drop(1)
          .toArray
      )

    actualArr should ===(expectedArr)
  }

  test("strings") {
    check(
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
        .scanLeftValues
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
