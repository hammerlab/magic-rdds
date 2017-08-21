package org.hammerlab.magic.rdd.scan

import cats.implicits.{ catsKernelStdGroupForInt, catsKernelStdMonoidForString }
import cats.kernel.Monoid
import org.hammerlab.magic.rdd.scan.ScanLeftValuesRDD._
import org.hammerlab.magic.rdd.scan.ScanLeftRDD._

import scala.reflect.ClassTag

abstract class ScanLeftRDDTest(inclusive: Boolean)
  extends ScanRDDTest {

  override def check[T: ClassTag](input: Iterable[T],
                                  expectedOpt: Option[Seq[T]] = None)(
      implicit
      m: Monoid[T]
  ): Unit = {

    val actualArr =
      sc
        .parallelize(input.toSeq, numPartitions)
        .scanLeft(inclusive)
        .collect()

    val expectedArr =
      expectedOpt.getOrElse(
        getExpected(input)
      )

    actualArr should be(expectedArr)
  }

  def stringsOutput: Seq[String]

  test("strings") {
    check(
      Seq("a", "bc", "", "def"),
      stringsOutput
    )
  }

  def byKeyOutput: Array[(String, Int)]

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
        .parallelize(seq, numPartitions)
        .scanLeftValues(inclusive)
        .collect()

    actual should be(
      byKeyOutput
    )
  }
}

abstract class ScanLeftRDDInclusiveTest(val numPartitions: Int)
  extends ScanLeftRDDTest(true) {

  override def getExpected[T](expected: Iterable[T])(implicit m: Monoid[T]): Seq[T] =
    expected
      .scanLeft(m.empty)(m.combine)
      .drop(1)
      .toList

  override def byKeyOutput: Array[(String, Int)] =
    Array(
      "a" →  1,
      "b" →  3,
      "c" →  6,
      "d" → 10,
      "e" → 15
    )

  override def stringsOutput: Seq[String] =
    Seq("a", "abc", "abc", "abcdef")
}

abstract class ScanLeftRDDExclusiveTest(val numPartitions: Int)
  extends ScanLeftRDDTest(false) {

  override def getExpected[T](expected: Iterable[T])(implicit m: Monoid[T]): Seq[T] =
    expected
      .scanLeft(m.empty)(m.combine)
      .dropRight(1)
      .toList

  override def byKeyOutput: Array[(String, Int)] =
    Array(
      "a" →  0,
      "b" →  1,
      "c" →  3,
      "d" →  6,
      "e" → 10
    )

  override def stringsOutput: Seq[String] =
    Seq("", "a", "abc", "abc")
}

class ScanLeftRDDInclusiveTest1 extends ScanLeftRDDInclusiveTest(1)
class ScanLeftRDDInclusiveTest4 extends ScanLeftRDDInclusiveTest(4)
class ScanLeftRDDInclusiveTest8 extends ScanLeftRDDInclusiveTest(8)

class ScanLeftRDDExclusiveTest1 extends ScanLeftRDDExclusiveTest(1)
class ScanLeftRDDExclusiveTest4 extends ScanLeftRDDExclusiveTest(4)
class ScanLeftRDDExclusiveTest8 extends ScanLeftRDDExclusiveTest(8)
