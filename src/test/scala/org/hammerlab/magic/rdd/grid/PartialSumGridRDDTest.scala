package org.hammerlab.magic.rdd.grid

import org.hammerlab.magic.util.SparkSuite

import scala.math.ceil

abstract class PartialSumGridRDDTest(n: Int) extends SparkSuite {
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

    import org.hammerlab.magic.rdd.grid.Monoids._

    val (gridRDD, partialSumsRDD, maxR, maxC) = PartialSumGridRDD(rdd, rHeight, cWidth)

    //val gridRDD = PartialSumGridRDD.rddToGridCDFRDD(rdd, rHeight, cWidth)
    //val partitioner = gridRDD.partitioner
    val partitioner = gridRDD.partitioner.get.asInstanceOf[GridPartitioner]

    val pRows = ceil(n * 1.0 / rHeight).toInt
    val pCols = ceil(n * 1.0 / cWidth).toInt

    partitioner.numPartitionRows should be(pRows)
    partitioner.numPartitionCols should be(pCols)
    partitioner.numPartitions should be(pRows * pCols)

    gridRDD.count should be(n * n)
    //gridRDD.rdd.count should be(n * n)

    //val cdf = gridRDD.partialSums2D(_ + _, 0).sortByKey().collect

    val partialSums = partialSumsRDD.sortByKey().collect
//      val expected =
//        (for {
//          (row, r) ← rawExpected.map(_.zipWithIndex).zipWithIndex
//          (t, c) ← row
//        } yield {
//          (n - 1 - r, c) → t
//        }).sortBy(_._1)

    val actual = partialSums.grouped(n).toArray.map(_.map(_._2))

    actual should be(expected)
  }

}

class GridCDFRDDTest4x4 extends PartialSumGridRDDTest(4) {

  def expectedStr =
    """
      |   6  3  1  0
      |  28 18 10  4
      |  66 45 27 12
      | 120 84 52 24
    """

  test("1-1") { testFn(1, 1) }
  test("2-2") { testFn(2, 2) }
  test("3-3") { testFn(3, 3) }
  test("4-4") { testFn(4, 4) }

  test("1-2") { testFn(1, 2) }
  test("2-1") { testFn(2, 1) }

  test("3-2") { testFn(3, 2) }
  test("2-3") { testFn(2, 3) }

  test("1-4") { testFn(1, 4) }
  test("4-1") { testFn(4, 1) }

  test("2-4") { testFn(2, 4) }
  test("4-2") { testFn(4, 2) }

  /*

      Input matrix:

         3   2 |  1   0
         7   6 |  5   4
        -------+-------
        11  10 |  9   8
        15  14 | 13  12

      After summing within each partition/block:

         5   2 |  1   0
        18   8 | 10   4
        -------+-------
        21  10 | 17   8
        50  24 | 42  20

      Output / Partial-summed matrix:

         6   3 |  1   0
        28  18 | 10   4
        -------+-------
        66  45 | 27  12
       120  84 | 52  24

   */
}

class GridCDFRDDTest10x10 extends PartialSumGridRDDTest(10) {

    /*

      Input matrix:

           9    8    7    6    5    4    3    2    1    0
          19   18   17   16   15   14   13   12   11   10
          29   28   27   26   25   24   23   22   21   20
          39   38   37   36   35   34   33   32   31   30
          49   48   47   46   45   44   43   42   41   40
          59   58   57   56   55   54   53   52   51   50
          69   68   67   66   65   64   63   62   61   60
          79   78   77   76   75   74   73   72   71   70
          89   88   87   86   85   84   83   82   81   80
          99   98   97   96   95   94   93   92   91   90

     */

  val expectedStr =
    """
      |   45   36   28   21   15   10    6    3    1    0
      |  190  162  136  112   90   70   52   36   22   10
      |  435  378  324  273  225  180  138   99   63   30
      |  780  684  592  504  420  340  264  192  124   60
      | 1225 1080  940  805  675  550  430  315  205  100
      | 1770 1566 1368 1176  990  810  636  468  306  150
      | 2415 2142 1876 1617 1365 1120  882  651  427  210
      | 3160 2808 2464 2128 1800 1480 1168  864  568  280
      | 4005 3564 3132 2709 2295 1890 1494 1107  729  360
      | 4950 4410 3880 3360 2850 2350 1860 1380  910  450
    """

  test("1-1") { testFn(1, 1) }
  test("3-3") { testFn(3, 3) }
  test("4-4") { testFn(4, 4) }
  test("10-10") { testFn(10, 10) }

  test("2-8") { testFn(2, 8) }
  test("8-2") { testFn(8, 2) }
}
