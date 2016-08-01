package org.hammerlab.magic.stats

import org.hammerlab.magic.iterator.RunLengthIterator
import spire.implicits._
import spire.math.{Integral, Numeric}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Wrapper for some computed statistics about a dataset of [[Numeric]] elements.
 *
 * @param n number of elements in the dataset.
 * @param mean mean.
 * @param stddev stddev.
 * @param mad median absolute deviation (from the median).
 * @param samplesOpt "sample" elements; the start and end of the data.
 * @param sortedSamplesOpt "sample" elements; the least and greatest elements. If the dataset is already sorted, meaning
 *                         this would be equivalent to [[samplesOpt]], it is omitted.
 * @param percentiles selected percentiles of the dataset.
 * @tparam K [[Numeric]] element type. TODO(ryan): allow this to be non-[[Numeric]].
 * @tparam V [[Integral]] value type.
 */
case class Stats[K: Numeric, V: Integral](n: V,
                                          mean: Double,
                                          stddev: Double,
                                          mad: Double,
                                          samplesOpt: Option[Samples[K, V]],
                                          sortedSamplesOpt: Option[Samples[K, V]],
                                          percentiles: Seq[(Double, Double)]) {

  def prettyDouble(d: Double): String = {
    if (math.floor(d).toInt == math.ceil(d).toInt)
      d.toInt.toString
    else
      "%.1f".format(d)
  }

  override def toString: String = {
    if (n == 0)
      "(empty)"
    else {
      val strings = ArrayBuffer[String]()

      strings +=
        List(
          s"num:\t$n",
          s"mean:\t${prettyDouble(mean)}",
          s"stddev:\t${prettyDouble(stddev)}",
          s"mad:\t${prettyDouble(mad)}"
        ).mkString(",\t")

      if (n > 0) {
        for {
          samples <- samplesOpt
          if samples.nonEmpty
        } {
          strings += s"elems:\t$samples"
        }

        for {
          sortedSamples <- sortedSamplesOpt
          if sortedSamples.nonEmpty
        } {
          strings += s"sorted:\t$sortedSamples"
        }
      }

      strings ++= percentiles.map(t => s"${prettyDouble(t._1)}:\t${prettyDouble(t._2)}")

      strings.mkString("\n")
    }
  }
}

/**
 * Helpers for constructing [[Stats]] / computing the statistics that populate a [[Stats]] instance.
 */
object Stats {

  /**
   * Construct a [[Stats]] from a sequence of "runs"; elements paired with a count of repetitions.
   *
   * @param v values.
   * @param numToSample highlight this many "runs" of data from the start and end of the data; likewise the least and
   *                    greatest elements (and repetition counts).
   * @param onlySampleSorted only highlight the least and greatest elements; omit the first and last
   */
  def fromHist[K: Numeric: Ordering, V: Integral](v: Iterable[(K, V)],
                                                  numToSample: Int = 10,
                                                  onlySampleSorted: Boolean = false): Stats[K, V] = {

    var alreadySorted = true
    val hist = mutable.HashMap[K, V]()
    var n = Integral[V].zero

    val values = {
      val vBuilder = Vector.newBuilder[(K, V)]
      var prevOpt: Option[K] = None
      for {
        (value, num) <- RunLengthIterator.reencode[K, V](v.iterator.buffered)
      } {
        if (alreadySorted) {
          if (prevOpt.exists(_ > value))
            alreadySorted = false
          else
            prevOpt = Some(value)
        }
        vBuilder += value -> num
        n += num
        hist.update(value, hist.getOrElse(value, Integral[V].zero) + num)
      }

      vBuilder.result()
    }

    if (values.isEmpty) {
      return empty
    }

    val sorted =
      if (alreadySorted)
        values
      else
        for { key <- hist.keys.toVector.sorted }
          yield
            key -> hist(key)

    val ps = histPercentiles(n, sorted)

    val median = ps(ps.length / 2)._2

    val medianDeviationsBuilder = Vector.newBuilder[(Double, V)]

    var sum = 0.0
    var sumSquares = 0.0
    for ((value, num) <- sorted) {
      val d = value.toDouble
      sum += d * num.toDouble()
      sumSquares += d * d * num.toDouble()
      medianDeviationsBuilder += math.abs(d - median) -> num
    }

    val medianDeviations = medianDeviationsBuilder.result().sortBy(_._1)

    val mad =
      getRunPercentiles(
        medianDeviations,
        Seq(50.0 -> (n.toDouble() - 1) / 2.0)
      ).head._2

    val mean = sum / n.toDouble()
    val stddev = math.sqrt(sumSquares / n.toDouble() - mean * mean)

    val samplesOpt =
      if (alreadySorted || !onlySampleSorted) {
        val firstElems = values.take(numToSample)
        val numFirstElems = firstElems.map(_._2).reduce(_ + _)

        val lastElems = values.takeRight(numToSample)
        val numLastElems = lastElems.map(_._2).reduce(_ + _)

        Some(Samples[K, V](n, firstElems, numFirstElems, lastElems, numLastElems))
      } else
        None

    val sortedSamplesOpt =
      if (!alreadySorted) {
        val leastElems = sorted.take(numToSample)
        val numLeastElems = leastElems.map(_._2).reduce(_ + _)

        val greatestElems = sorted.takeRight(numToSample)
        val numGreatestElems = greatestElems.map(_._2).reduce(_ + _)

        Some(Samples(n, leastElems, numLeastElems, greatestElems, numGreatestElems))
      } else
        None

    Stats(
      n,
      mean, stddev, mad,
      samplesOpt,
      sortedSamplesOpt,
      ps
    )
  }

  /**
   * Construct a [[Stats]] instance from input data `v`.
   * @param v values.
   * @param numToSample highlight this many "runs" of data from the start and end of the data; likewise the least and
   *                    greatest elements (and repetition counts).
   * @param onlySampleSorted only highlight the least and greatest elements; omit the first and last
   */
  def apply[K: Numeric: Ordering](v: Iterable[K],
                                  numToSample: Int = 10,
                                  onlySampleSorted: Boolean = false): Stats[K, Int] = {

    val vBuilder = Vector.newBuilder[K]
    var alreadySorted = true
    var prevOpt: Option[K] = None
    for (value <- v) {
      if (alreadySorted) {
        if (prevOpt.exists(_ > value))
          alreadySorted = false
        else
          prevOpt = Some(value)
      }
      vBuilder += value
    }

    val values = vBuilder.result()

    val n = values.length

    val sorted =
      if (alreadySorted)
        values
      else
        values.sorted

    val median = getMedian(sorted)

    val medianDeviationsBuilder = Vector.newBuilder[Double]

    var sum = 0.0
    var sumSquares = 0.0
    for (value <- sorted) {
      val d = value.toDouble
      sum += d
      sumSquares += d * d
      medianDeviationsBuilder += math.abs(d - median)
    }

    val medianDeviations = medianDeviationsBuilder.result().sorted
    val mad = getMedian(medianDeviations)

    val mean = sum / n
    val stddev = math.sqrt(sumSquares / n - mean * mean)

    val samplesOpt: Option[Samples[K, Int]] =
      if (alreadySorted || !onlySampleSorted) {
        // Count occurrences of the first N distinct values.
        val (firstElems, numFirstElems) = runLengthEncodeWithSum(values.iterator, numToSample)

        // Count occurrences of the last N distinct values.
        val (lastElems, numLastElems) = runLengthEncodeWithSum(values.reverseIterator, numToSample, reverse = true)

        Some(Samples(n, firstElems, numFirstElems, lastElems, numLastElems))
      } else
        None

    val sortedSamplesOpt: Option[Samples[K, Int]] =
      if (!alreadySorted) {
        // Count occurrences of the least N distinct values.
        val (leastElems, numLeastElems) = runLengthEncodeWithSum[K](sorted.iterator, numToSample)

        // Count occurrences of the greatest N distinct values.
        val (greatestElems, numGreatestElems) = runLengthEncodeWithSum(sorted.reverseIterator, numToSample, reverse = true)

        Some(Samples(n, leastElems, numLeastElems, greatestElems, numGreatestElems))
      } else
        None

    new Stats(
      n,
      mean, stddev, mad,
      samplesOpt,
      sortedSamplesOpt,
      percentiles(sorted)
    )
  }

  /**
   * Construct an empty [[Stats]] instance.
   */
  private def empty[K: Numeric, V: Integral]: Stats[K, V] =
    new Stats(
      n = Integral[V].zero,
      mean = 0,
      stddev = 0,
      mad = 0,
      samplesOpt = None,
      sortedSamplesOpt = None,
      percentiles = Nil
    )

  /**
   * Compute percentiles listed in `ps` of the data in `values`; wrapper for implementation below.
   */
  private def getRunPercentiles[K: Numeric, V: Integral](values: Seq[(K, V)],
                                                         ps: Seq[(Double, Double)]): Vector[(Double, Double)] =
    getRunPercentiles(
      values
        .iterator
        .buffered,
      ps
        .iterator
        .buffered
    ).toVector

  /**
   * Compute percentiles listed in `ps` of the data in `values`.
   *
   * @param values runs of elements.
   * @param percentiles percentiles to compute, specified as tuples where the key is the percentile and the value is the
   *                    index in `values` at which that percentile lies (interpolated to be a fractional amount between
   *                    two indices, where appropriate).
   * @return pairs of (percentile, value).
   */
  private def getRunPercentiles[K: Numeric, V: Integral](values: BufferedIterator[(K, V)],
                                                         percentiles: BufferedIterator[(Double, Double)]): Iterator[(Double, Double)] =
    new Iterator[(Double, Double)] {

      var elemsPast = 0.0
      var curK: Option[Double] = None

      override def hasNext: Boolean = percentiles.hasNext

      override def next(): (Double, Double) = {
        val (percentile, idx) = percentiles.next()
        while(elemsPast <= idx) {
          val (k, v) = values.next()
          curK = Some(k.toDouble())
          elemsPast += v.toDouble()
        }

        val distancePast = elemsPast - idx

        percentile ->
          (
            if (distancePast < 1)
              curK.get * distancePast + values.head._1.toDouble() * (1 - distancePast)
            else
              curK.get
          )
      }
    }

  /**
   * Compute some relevant percentiles based on the number of elements present.
   * @return pairs of (percentile, value).
   */
  private def histPercentiles[K: Numeric, V: Integral](N: V, values: IndexedSeq[(K, V)]): Vector[(Double, Double)] = {
    val n = N - 1
    val denominators: Iterator[Int] = Iterator(2, 4, 10, 20, 100, 1000, 10000)

    val nd = n.toDouble
    val percentileIdxs =
      denominators
        .takeWhile(d => d <= n || d == 2)  // Always take the median (denominator 2 aka 50th percentile).
        .flatMap(d => {
          val loPercentile = 100.0 / d
          val hiPercentile = 100.0 - loPercentile

          val loIdx = nd / d
          val hiIdx = nd - loIdx

          if (d == 2)
          // Median (50th percentile, denominator 2) only emits one tuple.
            Iterator(loPercentile -> loIdx)
          else
          // In general, we emit two tuples per "denominator", one on the high side and one on the low. For example, for
          // denominator 4, we emit the 25th and 75th percentiles.
            Iterator(loPercentile -> loIdx, hiPercentile -> hiIdx)
        })
        .toArray
        .sortBy(_._1)

    getRunPercentiles(values, percentileIdxs)
  }

  /**
   * Compute some relevant percentiles based on the number of elements present.
   * @return pairs of (percentile, value).
   */
  private def percentiles[T: Numeric](values: IndexedSeq[T]): Vector[(Double, Double)] = {
    val n = values.length - 1

    val denominators: Iterator[Int] = {
      lazy val pow10s: Stream[Int] = 100 #:: pow10s.map(_ * 10)
      Iterator(2, 4, 10, 20) ++ pow10s.iterator
    }

    val nd = n.toDouble
    denominators.takeWhile(_ <= n).flatMap(d => {
      val loPercentile = 100.0 / d
      val hiPercentile = 100.0 - loPercentile

      val loFrac = nd / d

      val loFloor = math.floor(loFrac).toInt
      val loCeil = math.ceil(loFrac).toInt

      val hiFloor = n - loFloor
      val hiCeil = n - loCeil

      val loRemainder = loFrac - loFloor
      val (lo, hi) =
        if (loFloor == loCeil)
          (values(loFloor).toDouble(), values(hiFloor).toDouble())
        else
          (
            values(loFloor).toDouble() * loRemainder +  values(loCeil).toDouble() * (1 - loRemainder),
            values(hiCeil).toDouble() * loRemainder + values(hiFloor).toDouble() * (1 - loRemainder)
          )

      if (d == 2)
        // Median (50th percentile, denominator 2) only emits one tuple.
        Iterator(loPercentile -> lo)
      else
        // In general, we emit two tuples per "denominator", one on the high side and one on the low. For example, for
        // denominator 4, we emit the 25th and 75th percentiles.
        Iterator(loPercentile -> lo, hiPercentile -> hi)

    }).toVector.sortBy(_._1)
  }

  private def getMedian[T: Numeric](sorted: Vector[T]): Double = {
    val n = sorted.length
    if (n == 0)
      -1
    else if (n % 2 == 0)
      (sorted(n / 2 - 1) + sorted(n / 2)).toDouble() / 2.0
    else
      sorted(n / 2).toDouble()
  }

  /**
   * Find the first `N` "runs" from the beginning of `it`. If `reverse`, return them in reversed order.
   */
  private def runLengthEncodeWithSum[K: Numeric](it: Iterator[K],
                                                 N: Int,
                                                 reverse: Boolean = false): (Seq[(K, Int)], Int) = {
    var sum = 0
    var i = 0
    val runs = ArrayBuffer[(K, Int)]()
    val runLengthIterator = RunLengthIterator[K](it)
    while (i < N && runLengthIterator.hasNext) {
      val (elem, count) = runLengthIterator.next()

      if (reverse)
        runs.prepend(elem -> count)
      else
        runs += ((elem, count))

      sum += count
      i += 1
    }
    runs -> sum
  }
}
