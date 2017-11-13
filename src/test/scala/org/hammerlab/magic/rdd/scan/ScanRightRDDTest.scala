package org.hammerlab.magic.rdd.scan

import cats.Monoid
import cats.implicits.{ catsKernelStdGroupForInt, catsKernelStdMonoidForString }
import magic_rdds.scan._

import scala.reflect.ClassTag

abstract class ScanRightRDDTest(useRDDReversal: Boolean)
  extends ScanRDDTest {

  def inclusive: Boolean

  val byKeysOutput: Seq[(String, Int)]
  val stringsOutput: Seq[String]

  test("strings") {
    check(
      Seq("a", "bc", "", "def"),
      stringsOutput
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

    val rdd = sc.parallelize(seq, numPartitions)

    val scanned =
      (inclusive, useRDDReversal) match {
        case ( true,  true) ⇒ rdd.scanRightValuesInclusive
        case ( true, false) ⇒ rdd.scanRightValuesInclusive(useRDDReversal)
        case (false,  true) ⇒ rdd.scanRightValues
        case (false, false) ⇒ rdd.scanRightValues(useRDDReversal)
      }

    scanned.collect should be(
      byKeysOutput
    )
  }

  def check[T: ClassTag](input: Iterable[T],
                         expectedOpt: Option[Seq[T]] = None)(
      implicit m: Monoid[T]
  ): Unit = {

    val rdd = sc.parallelize(input.toSeq)

    val scanned =
      (inclusive, useRDDReversal) match {
        case ( true,  true) ⇒ rdd.scanRightInclusive
        case ( true, false) ⇒ rdd.scanRightInclusive(useRDDReversal)
        case (false,  true) ⇒ rdd.scanRight
        case (false, false) ⇒ rdd.scanRight(useRDDReversal)
      }

    val expectedArr =
      expectedOpt.getOrElse(
        getExpected(input)
      )

    scanned.collect should be(expectedArr)
  }
}

trait ScanRightInclusiveTest {
  self: ScanRightRDDTest ⇒

  override def inclusive: Boolean = true

  override def getExpected[T](expected: Iterable[T])(implicit m: Monoid[T]): Seq[T] =
    expected
      .scanRight(m.empty)(m.combine)
      .dropRight(1)
      .toList

  override val byKeysOutput: Seq[(String, Int)] =
    Array(
      "a" → 15,
      "b" → 14,
      "c" → 12,
      "d" →  9,
      "e" →  5
    )

  override val stringsOutput: Seq[String] =
    Seq("abcdef", "bcdef", "def", "def")
}

trait ScanRightExclusiveTest {
  self: ScanRightRDDTest ⇒

  override def inclusive: Boolean = false

  override def getExpected[T](expected: Iterable[T])(implicit m: Monoid[T]): Seq[T] =
    expected
      .scanRight(m.empty)(m.combine)
      .drop(1)
      .toList

  override val byKeysOutput: Seq[(String, Int)] =
    Array(
      "a" → 14,
      "b" → 12,
      "c" →  9,
      "d" →  5,
      "e" →  0
    )

  override val stringsOutput: Seq[String] =
    Seq("bcdef", "def", "def", "")
}

abstract class ScanRightRDDReversingInclusiveTest(override val numPartitions: Int)
  extends ScanRightRDDTest(true)
    with ScanRightInclusiveTest

abstract class ScanRightRDDReversingExclusiveTest(override val numPartitions: Int)
  extends ScanRightRDDTest(true)
    with ScanRightExclusiveTest

abstract class ScanRightRDDMaterializingInclusiveTest(override val numPartitions: Int)
  extends ScanRightRDDTest(false)
    with ScanRightInclusiveTest

abstract class ScanRightRDDMaterializingExclusiveTest(override val numPartitions: Int)
  extends ScanRightRDDTest(false)
    with ScanRightExclusiveTest

class ReversingInclusiveTest1 extends ScanRightRDDReversingInclusiveTest(1)
class ReversingInclusiveTest4 extends ScanRightRDDReversingInclusiveTest(4)
class ReversingInclusiveTest8 extends ScanRightRDDReversingInclusiveTest(8)

class ReversingExclusiveTest1 extends ScanRightRDDReversingExclusiveTest(1)
class ReversingExclusiveTest4 extends ScanRightRDDReversingExclusiveTest(4)
class ReversingExclusiveTest8 extends ScanRightRDDReversingExclusiveTest(8)

class MaterializingInclusiveTest1 extends ScanRightRDDMaterializingInclusiveTest(1)
class MaterializingInclusiveTest4 extends ScanRightRDDMaterializingInclusiveTest(4)
class MaterializingInclusiveTest8 extends ScanRightRDDMaterializingInclusiveTest(8)

class MaterializingExclusiveTest1 extends ScanRightRDDMaterializingExclusiveTest(1)
class MaterializingExclusiveTest4 extends ScanRightRDDMaterializingExclusiveTest(4)
class MaterializingExclusiveTest8 extends ScanRightRDDMaterializingExclusiveTest(8)

