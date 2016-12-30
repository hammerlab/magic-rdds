package org.hammerlab.magic.rdd.sliding

import org.hammerlab.magic.rdd.sliding.SlidingRDD._
import org.hammerlab.spark.test.suite.SparkSuite

class SlidingRDDTest extends SparkSuite {

  def test2N(n: Int): Unit = {
    def lToT(l: IndexedSeq[Int]): (Int, Int) = (l(0), l(1))
    test(s"two:$n") {
      val range = 1 to n
      var expectedSlid = range.sliding(2).map(lToT).toArray

      sc.parallelize(range).sliding2().collect should ===(expectedSlid)

      expectedSlid ++= Array((n, 0))

      sc.parallelize(range).sliding2(0).collect should ===(expectedSlid)
    }
  }

  test2N(100)
  test2N(12)
  test2N(11)
  test2N(10)
  test2N(9)
  test2N(8)

  def test3N(n: Int): Unit = {
    def lToT(l: IndexedSeq[Int]): (Int, Int, Int) = (l(0), l(1), l(2))
    test(s"three:$n") {
      val range = 1 to n
      var expectedSlid = range.sliding(3).map(lToT).toArray

      sc.parallelize(range).sliding3().collect should ===(expectedSlid)

      expectedSlid ++= Array((n - 1, n, 0), (n, 0, 0))

      sc.parallelize(range).sliding3(0).collect should ===(expectedSlid)
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
        val actual = sc.parallelize(range).sliding(n).collect.map(_.toArray)
        val expected = range.sliding(n).map(_.toArray).toSeq
        str(actual) should ===(str(expected))
      }
      {
        val actual = sc.parallelize(range).sliding(n, 0).collect.map(_.toArray)
        val expected = paddedRange.sliding(n).map(_.toArray).toSeq
        str(actual) should ===(str(expected))
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
    s.indices.map(i => {
      var j = s.indexOf('$', i)
      if (j < 0) {
        j = s.length
      }
      s.substring(i, j)
    })

  def testSlideUntil(in: String): Unit = {
    val s = in.stripMargin.trim.split("\n").mkString("")
    val actual = sc.parallelize(s).slideUntil('$').map(_.mkString("")).collect.toList
    val expected = getExpected(s)
    actual should ===(expected)
  }

  test("until:1:1") {
    testSlideUntil(
      """
        |$AA$$GG$ATG$TGAGACGCTCGC$
        |$$G$$AGCT$GGGTGAAC$$CGCTA
        |$G$TTCGGAGTGGC$CTTGTG$$AC
        |GT$AGAAAGTGG$TTT$TGC$ATAC
        |"""
    )
  }

  test("until:2:1") {
    testSlideUntil(
      """
        |ACCGC$GAC$TA$CATCTGGTCCCT
        |GTGAAGAG$AGTTGCCCCTTAGG$$
        |GTCGT$$GTGTT$CTGACG$$GCCA
        |CGGCTAT$TCGCGTGGTTCA$CCGC
        |"""
    )
  }

  test("until:1:2") {
    testSlideUntil(
      """
        |TT$TA$AATCGGAG$$GG$T$GA$G
        |TAAC$G$GG$$CCTGGTT$$TGGG$
        |CA$CC$$$$G$TA$CA$TGCCAAGC
        |$$T$$T$$G$$GG$CTCCC$CAA$$
      """
    )
  }

  test("small") {
    testSlideUntil("$AC$AAA$AAA$")
  }

}
