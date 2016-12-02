package org.hammerlab.magic.rdd.keyed

import org.hammerlab.magic.rdd.keyed.SplitByKeyRDD._
import org.hammerlab.spark.test.suite.SparkSuite

import scala.reflect.ClassTag

class SplitByKeyRDDTest extends SparkSuite {

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

  def check[T: ClassTag](ints: Seq[Int], numPartitions: Int, keyFn: Int => T)(expected: (T, Vector[Int], Int)*) = {
    val rdd = sc.parallelize(ints, numPartitions).map(i => keyFn(i) -> i)

    val perKeyRDDs = rdd.splitByKey()

    val expectedElems =
      (for {
        (k, elems, _) <- expected
      } yield
        k -> elems
      ).toMap

    perKeyRDDs.mapValues(_.collect().toVector.sorted) should be(expectedElems)

    for {
      (k, _, keyPartitions) <- expected
      rdd = perKeyRDDs(k)
    } {
      withClue(s"RDD for key $k: ") {
        rdd.getNumPartitions should be(keyPartitions)
      }
    }
  }

  test("10-4-2") {
    check(
      ints10, 4, _ % 2
    )(
      (0, Vector(0, 4, 6), 1),
      (1, Vector(1, 3, 3, 3, 5, 5, 7), 3)
    )
  }

  test("10-4-1") {
    check(
      ints10, 4, _ >= 0
    )(
      (true, Vector(0, 1, 3, 3, 3, 4, 5, 5, 6, 7), 4)
    )
  }

  test("100-4-10") {
    check(
      ints100, 4, x => x
    )(
      (0, Vector.fill(15)(0), 1),
      (1, Vector.fill(10)(1), 1),
      (2, Vector.fill( 5)(2), 1),
      (3, Vector.fill( 8)(3), 1),
      (4, Vector.fill(10)(4), 1),
      (5, Vector.fill(13)(5), 1),
      (6, Vector.fill(11)(6), 1),
      (7, Vector.fill(11)(7), 1),
      (8, Vector.fill( 8)(8), 1),
      (9, Vector.fill( 9)(9), 1)
    )
  }
}
