package org.hammerlab.magic.rdd.scan

import org.hammerlab.spark.test.suite.SparkSuite

import scala.reflect.ClassTag

trait ScanRDDTest extends SparkSuite {

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

  def check(input: Iterable[Int]): Unit =
    check[Int](0, input, (a: Int, b: Int) ⇒ a + b)

  def check[T: ClassTag](identity: T,
                         input: Iterable[T],
                         expected: Seq[T])(implicit op: (T, T) ⇒ T): Unit =
    check(identity, input, op, Some(expected))

  def check[T: ClassTag](identity: T,
                         input: Iterable[T],
                         op: (T, T) ⇒ T,
                         expectedOpt: Option[Seq[T]] = None): Unit
}

object Ops {
  // This needs to live outside of a Suite because it gets serialized, and Suites are not Serializable.
  implicit def add(a: String, b: String): String = a + b
}
