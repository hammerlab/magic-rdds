package org.hammerlab.magic.rdd.scan

import cats.Monoid
import cats.instances.int.catsKernelStdGroupForInt
import org.hammerlab.spark.test.suite.SparkSuite

import scala.reflect.ClassTag

import magic_rdds.ScanRDD

trait ScanRDDTest
  extends SparkSuite {

  def getExpected[T](expected: Iterable[T])(implicit m: Monoid[T]): Seq[T]

  def numPartitions: Int

  test( "0") { check(1 to  0) }
  test( "1") { check(1 to  1) }
  test( "2") { check(1 to  2) }
  test( "3") { check(1 to  3) }
  test( "4") { check(1 to  4) }
  test( "5") { check(1 to  5) }
  test( "6") { check(1 to  6) }
  test( "7") { check(1 to  7) }
  test( "8") { check(1 to  8) }
  test( "9") { check(1 to  9) }
  test("10") { check(1 to 10) }

  def check[T: ClassTag](input: Iterable[T],
                         expected: Seq[T])(
      implicit
      m: Monoid[T]
  ): Unit =
    check(
      input,
      Some(expected)
    )

  def check[T: ClassTag](input: Iterable[T],
                         expectedOpt: Option[Seq[T]] = None)(
                            implicit
                            m: Monoid[T]
                        ): Unit
}
