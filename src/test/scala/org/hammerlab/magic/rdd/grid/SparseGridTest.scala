package org.hammerlab.magic.rdd.grid

class SparseGridTest
  extends PrefixSumTest(4) {

  /*
      Input:

        - 4 - -
        1 - - -
        - - 2 -
        - - - 3
   */

  override val input =
    Vector(
      3 → 1 → 4,
      2 → 0 → 1,
      1 → 2 → 2,
      0 → 3 → 3
    )

  override def expectedStr: String =
    """
      |  4 4 0 0
      |  5 4 0 0
      |  7 6 2 0
      | 10 9 5 3
    """

  test("1-1") { testFn(1, 1) }
  test("1-2") { testFn(1, 2) }
  test("2-1") { testFn(2, 1) }
  test("2-2") { testFn(2, 2) }
}
