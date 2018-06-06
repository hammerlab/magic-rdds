package org.hammerlab.magic.rdd.sliding

import hammerlab.iterator._
import magic_rdds._
import org.apache.spark.rdd.RDD
import org.hammerlab.spark.test.rdd.Util.makeRDD
import org.hammerlab.spark.test.suite.SparkSuite

class SlidingRDDTest
  extends SparkSuite {

  /**
   * Make an RDD of auto-incrementing integers starting from 0 and with partition-sizes given by `sizes`
   */
  def make(sizes: Int*): RDD[Int] =
    makeRDD(
      sizes
        .scanLeft(0)(_ + _)
        .sliding2
        .map {
          case (start, end) ⇒
            start until end
        }
        .toList: _*
    )

  def test2N(n: Int): Unit = {
    def lToT(l: IndexedSeq[Int]): (Int, Int) = (l(0), l(1))
    test(s"two:$n") {
      val range = 1 to n
      var expectedSlid = range.sliding2.toArray

      ==(
        sc
          .parallelize(range)
          .sliding2
          .collect,
        expectedSlid
      )
    }
  }

  test2N(100)
  test2N(12)
  test2N(11)
  test2N(10)
  test2N(9)
  test2N(8)

  {
    lazy val rdd =
      make(
        0,
        1,
        0,
        0,
        2,
        0,
        1,
        1,
        0
      )

    test("sliding2") {
      ==(rdd.sliding2    .collect(), (0 until 5).sliding2    .toArray)
      ==(rdd.sliding2Next.collect(), (0 until 5).sliding2Opt .toArray)
      ==(rdd.sliding2Prev.collect(), (0 until 5).sliding2Prev.toArray)
    }

    test("sliding3") {
      ==(rdd.sliding3    .collect(), (0 until 5).sliding3        .toArray)
      ==(rdd.sliding3Opt .collect(), (0 until 5).sliding3Opt     .toArray)
      ==(rdd.sliding3Next.collect(), (0 until 5).sliding3NextOpts.toArray)
    }

    test("sliding4") {
      ==(rdd.sliding(4).collect(), (0 until 5).sliding(4).toArray)
      ==(
        rdd.sliding(4, includePartial = true).collect(),
        Array(
          0 to 3,
          1 to 4,
          2 to 4,
          3 to 4,
          4 to 4
        )
      )

      ==(
        rdd.window(numPrev = 0, numNext = 3).collect(),
        Array(
          Window(Nil, 0, 1 to 3),
          Window(Nil, 1, 2 to 4),
          Window(Nil, 2, 3 to 4),
          Window(Nil, 3, 4 to 4),
          Window(Nil, 4, Nil)
        )
      )
    }

    test("sliding5") {
      ==(rdd.sliding(5).collect(), Array(0 to 4))
      ==(
        rdd.sliding(5, includePartial = true).collect(),
        Array(
          0 to 4,
          1 to 4,
          2 to 4,
          3 to 4,
          4 to 4
        )
      )
    }

    test("sliding6") {
      ==(
        rdd.sliding(6).collect()
      )(
        Array()
      )
      ==(
        rdd.sliding(6, includePartial = true).collect(),
        Array(
          0 to 4,
          1 to 4,
          2 to 4,
          3 to 4,
          4 to 4
        )
      )
    }
  }

  def test3N(n: Int): Unit = {
    def lToT(l: IndexedSeq[Int]): (Int, Int, Int) = (l(0), l(1), l(2))
    test(s"three:$n") {
      val range = 1 to n
      var expectedSlid = range.sliding(3).map(lToT).toArray

      ==(sc.parallelize(range).sliding3.collect, expectedSlid)
    }
  }

  test3N(100)
  test3N(12)
  test3N(11)
  test3N(10)
  test3N(9)
  test3N(8)

  def str(s: Traversable[Array[Int]]) = s.map(_.mkString(",")).mkString(" ")

  def testN(n: Int, k: Int): Unit = {
    test(s"$n:$k") {
      val range = 1 to k
      val paddedRange: Iterable[Int] = range ++ Array.fill(n - 1)(0)

      {
        val actual =
          sc
            .parallelize(range)
            .sliding(n)
            .collect
            .map(_.toArray)

        val expected =
          range
            .sliding(n)
            .map(_.toArray)
            .toSeq
        ==(str(actual), str(expected))
      }
    }
  }

  testN(1, 100)
  testN(1, 12)
  testN(1, 11)
  testN(1, 10)
  testN(1, 9)
  testN(1, 8)

  testN(2, 100)
  testN(2, 12)
  testN(2, 11)
  testN(2, 10)
  testN(2, 9)
  testN(2, 8)
  testN(2, 5)
  testN(2, 4)

  testN(3, 100)
  testN(3, 12)
  testN(3, 11)
  testN(3, 10)
  testN(3, 9)
  testN(3, 8)

  testN(4, 100)
  testN(4, 16)
  testN(4, 15)
  testN(4, 14)
  testN(4, 13)
  testN(4, 12)

  def getExpected(s: String): Seq[String] =
    s.indices.map { i ⇒
      var j = s.indexOf('$', i)
      if (j < 0) {
        j = s.length
      }
      s.substring(i, j)
    }
}
