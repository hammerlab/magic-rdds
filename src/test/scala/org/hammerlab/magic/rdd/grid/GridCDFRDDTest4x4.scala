package org.hammerlab.magic.rdd.grid

class GridCDFRDDTest4x4
  extends PrefixSumTest(4) {

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
