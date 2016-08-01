package org.hammerlab.magic.math

import org.apache.commons.math3.util.FastMath

import scala.collection.mutable.ArrayBuffer

/**
 * Implementation of a hypergeometric distribution, modeled after
 * [[org.apache.commons.math3.distribution.HypergeometricDistribution]], but supporting [[Long]] parameters.
 * @param N Population size.
 * @param K Number of successes.
 * @param n Number to sample.
 */
case class HyperGeometricDistribution(N: Long, K: Long, n: Int) {

  // These will be filled with n+1 elements corresponding to the PDF and CDF values for k ∈ [0, n].
  val pdf = ArrayBuffer[Double]()
  val cdf = ArrayBuffer[Double]()

  // This will be set to the log of the binomial coefficient C(N, n), which is used multiple times in subsequent
  // calculations.
  var d = 0.0

  // logs of k!, for k in [0, n].
  val logBinomPartialSumsLo = ArrayBuffer[Double]()

  // logs of K! / (K - k)!, for k in [0, n].
  val logBinomPartialSumsK = ArrayBuffer[Double]()

  // logs of (N - K)! / (N - K - k)!, for k in [0, n].
  val logBinomPartialSumsNK = ArrayBuffer[Double]()

  // Compute log-arrays described above.
  (0 to n).foreach(k => {
    if (k == 0) {
      logBinomPartialSumsLo += 0
      logBinomPartialSumsK += 0
      logBinomPartialSumsNK += 0
    } else {
      logBinomPartialSumsLo += (logBinomPartialSumsLo(k - 1) + FastMath.log(k))
      logBinomPartialSumsK += (logBinomPartialSumsK(k - 1) + FastMath.log(K + 1 - k))
      logBinomPartialSumsNK += (logBinomPartialSumsNK(k - 1) + FastMath.log(N - K + 1 - k))

      d += FastMath.log(N + 1 - k)
      d -= FastMath.log(k)
    }
  })

  // Compute PDF and CDF.
  (0 to n).foreach(k => {
    val p1 = logBinomPartialSumsK(k) - logBinomPartialSumsLo(k)
    val p2 = logBinomPartialSumsNK(n - k) - logBinomPartialSumsLo(n - k)
    val v = FastMath.exp(p1 + p2 - d)
    pdf += v
    if (k == 0)
      cdf += v
    else
      cdf += (v + cdf(k - 1))
  })

  // Given a double x in [0, 1], binary-search the CDF to find the greatest integer k such that CDF(k) ≤ x.
  def invCDF(x: Double, start: Int = 0, end: Int = n): Int = {
    if (start == end)
      start
    else {
      val mid = (start + end) / 2
      val c = cdf(mid)
      if (x <= c)
        invCDF(x, start, mid)
      else
        invCDF(x, mid + 1, end)
    }
  }
}
