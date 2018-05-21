package org.hammerlab.magic.rdd.keyed

import magic_rdds.collect._
import magic_rdds.keyed._
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.Cmp

import scala.Vector.fill
import scala.reflect.ClassTag

class SplitByKeyTest
  extends SparkSuite {

  val ints10 = Vector(4, 3, 5, 6, 0, 5, 3, 1, 3, 7)

  val ints100 =
    Vector(
      4, 3, 5, 6, 0, 5, 3, 0, 3, 7,
      5, 0, 4, 8, 6, 3, 6, 9, 1, 8,
      1, 1, 6, 5, 1, 3, 5, 5, 0, 5,
      4, 2, 8, 3, 2, 2, 4, 0, 5, 7,
      0, 5, 9, 6, 0, 0, 5, 1, 7, 1,
      7, 8, 8, 7, 4, 4, 1, 9, 0, 9,
      7, 1, 6, 7, 9, 0, 4, 0, 6, 0,
      0, 8, 4, 6, 8, 6, 2, 9, 6, 0,
      9, 3, 7, 7, 6, 4, 1, 9, 5, 8,
      2, 5, 7, 3, 1, 4, 7, 9, 5, 0
    )

  def check[K: ClassTag : Cmp](
    elems: Seq[Int],
    numPartitions: Int,
    keyFn: Int ⇒ K
  )(
    expected: (K, Seq[Seq[Int]])*
  ):
    Unit =
    check(
      elems
        .map {
          i ⇒
            keyFn(i) → i
        },
      numPartitions
    )(
      expected: _*
    )

  def check[
    K: ClassTag : Cmp,
    V: ClassTag : Cmp
  ](
    elems: Seq[(K, V)],
    numPartitions: Int
  )(
    expected: (K, Seq[Seq[V]])*
  ):
    Unit = {
    val rdd = sc.parallelize(elems, numPartitions)

    val perKeyRDDs = rdd.splitByKey

    ==(
      perKeyRDDs
        .mapValues(
          _
            .collectParts
            .map(_.toList)
            .toList
        ),
      expected.toMap
    )
  }

  test("10-4-2") {
    check(
      ints10, 4, _ % 2
    )(
      0 →
        Seq(
          Seq(4, 6, 0)
        ),
      1 →
        Seq(
          Seq(3, 5, 5),
          Seq(3, 1, 3),
          Seq(7)
        )
    )
  }

  test("10-4-1") {
    check(
      ints10, 4, _ >= 0
    )(
      true →
        Seq(
          Seq(4, 3, 5),
          Seq(6, 0, 5),
          Seq(3, 1, 3),
          Seq(7)
        )
    )
  }

  test("100-4-10") {
    check(
      ints100, 4, x ⇒ x
    )(
      0 → Seq(fill(15)(0)),
      1 → Seq(fill(10)(1)),
      2 → Seq(fill( 5)(2)),
      3 → Seq(fill( 8)(3)),
      4 → Seq(fill(10)(4)),
      5 → Seq(fill(13)(5)),
      6 → Seq(fill(11)(6)),
      7 → Seq(fill(11)(7)),
      8 → Seq(fill( 8)(8)),
      9 → Seq(fill( 9)(9))
    )
  }

  test("100-15-10") {
    check(
      ints100.zipWithIndex, 15
    )(
      0 → Seq(Seq( 4,  7, 11, 28, 37, 40, 44), Seq(45, 58, 65, 67, 69, 70, 79), Seq(99)),
      1 → Seq(Seq(18, 20, 21, 24, 47, 49, 56), Seq(61, 86, 94)),
      2 → Seq(Seq(31, 34, 35, 76, 90)),
      3 → Seq(Seq( 1,  6,  8, 15, 25, 33, 81), Seq(93)),
      4 → Seq(Seq( 0, 12, 30, 36, 54, 55, 66), Seq(72, 85, 95)),
      5 → Seq(Seq( 2,  5, 10, 23, 26, 27, 29), Seq(38, 41, 46, 88, 91, 98)),
      6 → Seq(Seq( 3, 14, 16, 22, 43, 62, 68), Seq(73, 75, 78, 84)),
      7 → Seq(Seq( 9, 39, 48, 50, 53, 60, 63), Seq(82, 83, 92, 96)),
      8 → Seq(Seq(13, 19, 32, 51, 52, 71, 74), Seq(89)),
      9 → Seq(Seq(17, 42, 57, 59, 64, 77, 80), Seq(87, 97))
    )
  }
}
