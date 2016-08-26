package org.hammerlab.magic.math

import math.{exp, log, max, min}

/**
 * Some utilities for generating exponential sequences of integers that can be used as e.g. histogram-bucket boundaries.
 */
object Steps {

  /**
   * Divide [0, maxDepth] into N geometrically-evenly-spaced steps (of size ≈maxDepth^(1/N)).
   *
   * Until the k-th step is bigger than k, the whole number k is used in its stead.
   */
  def geometricEvenSteps(maxDepth: Int, N: Int = 100): Set[Int] = {
    val logMaxDepth = log(maxDepth)

    Set(0) ++
      (for {
        i ← 1 until N
      } yield
        min(
          maxDepth,
          max(
            i,
            exp(
              (i - 1) * logMaxDepth / (N - 2)
            ).toInt
          )
        )
      ).toSet
  }

  /**
   * Produce a set of "round numbers" between 0 and a provided N, inclusive.
   *
   * Coverage is relatively dense but the total number of sampled/returned integers is still O(log(N)) in the input N;
   * specifically, 35 integers are returned in each factor-of-10 window (detailed below).
   *
   * The absolute difference between consecutive integers is non-decreasing over the entire range and, (after the [0,10]
   * interval), no two consecutive integers returned are more than 10% different from one another.
   *
   *
   *  0,  1,  2,  3,  4,  5,  6,  7,  8,  9,    base case: include all of [0, 10].
   * 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,    step by one from 10% to 20% of the next power of 10 (100 here).
   * 20,     22,     24,     26,     28,
   * 30,     32,     34,     36,     38,
   * 40,     42,     44,     46,     48,        step by two from 20% to 50% of the next power of 10.
   * 50,                 55,
   * 60,                 65,
   * 70,                 75,
   * 80,                 85,
   * 90,                 95,                    step by five from 50% to 100% of the next power of 10.
   *
   * …then repeat the [10, 95] portion, multiplied by powers of 10:
   *
   * 100, 110, 120, 130, 140, 150, 160, 170, 180, 190,      this is 10x the "steps by one" section above.
   * 200,      220,      240,      260,      280,
   * 300,      320,      340,      360,      380,           likewise, 10x the "steps by two" from above.
   *
   * …etc.
   */
  def roundNumbers(maxDepth: Int): Set[Int] =
    (0 until 10).toSet ++
      RoundNumbers(
        (10 until 20) ++ (20 until 50 by 2) ++ (50 until 100 by 5),
        maxDepth,
        10
      ).toSet
}
