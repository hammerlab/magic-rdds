package org.hammerlab.magic.rdd.grid

import org.hammerlab.spark.test.suite.SparkSuite

import hammerlab.math.utils.ceil

abstract class PrefixSumTest(n: Int)
  extends SparkSuite {

  /**
   * Build an n-by-n grid of the integers in [0, n*n).
   *
   * For example, if n=4:
   *
   *      +-----------+
   *    3 |12 13 14 15|
   *    2 | 8  9 10 11|
   *    1 | 4  5  6  7|
   *    0 | 0  1  2  3|
   *      +-----------+
   *        0  1  2  3
   */
  val input =
    for {
      r ← 0 until n
      c ← 0 until n
    } yield {
      (r, c) → (n * (n - 1 - r) + (n - 1 - c))  // aka: n^2 - 1 - nr - c
    }

  def expectedStr: String

  // Parse a matrix of expected values from an ASCII-art grid.
  def expected =
    expectedStr
      .trim
      .stripMargin
      .split("\n")
      .reverse
      .map(
        _
          .trim
          .split("\\s+")
          .map(_.toInt)
      )

  def testFn(rHeight: Int, cWidth: Int): Unit = {
    val rdd = sc.parallelize(input)

    import cats.implicits.catsKernelStdGroupForInt

    val Result(pdf, cdf, maxR, maxC) = Result(rdd, rHeight, cWidth)

    val partitioner = pdf.partitioner.get.asInstanceOf[GridPartitioner]

    val pRows = ceil(n, rHeight)
    val pCols = ceil(n, cWidth)

    ==(partitioner.numPartitionRows, pRows)
    ==(partitioner.numPartitionCols, pCols)
    ==(partitioner.numPartitions, pRows * pCols)

    ==(pdf.count, input.length)

    val partialSums = cdf.sortByKey().collect

    val actual = partialSums.grouped(n).toArray.map(_.map(_._2))

    ==(actual, expected)
  }
}
