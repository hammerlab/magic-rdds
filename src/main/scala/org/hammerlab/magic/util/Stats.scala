package org.hammerlab.magic.util

import org.hammerlab.magic.iterator.RunLengthIterator
import spire.implicits._
import spire.math.{Numeric, Integral}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class Runs[K, V: Integral](elems: Seq[(K, V)]) {
  override def toString: String =
    (
      for ((elem, count) <- elems) yield
        if (count == 1)
          elem.toString
        else
          s"$elem×$count"
    ).mkString(", ")
}

object Runs {
  implicit def runsToSeq[K, V: Integral](runs: Runs[K, V]): Seq[(K, V)] = runs.elems
  implicit def seqToRuns[K, V: Integral](elems: Seq[(K, V)]): Runs[K, V] = Runs(elems)
}

case class Samples[K, V: Integral](n: V, first: Runs[K, V], numFirst: V, last: Runs[K, V], numLast: V) {
  def isEmpty: Boolean = first.isEmpty
  def nonEmpty: Boolean = first.nonEmpty

  def removeOverlap(num: V, first: Runs[K, V], last: Runs[K, V]): Runs[K, V] = {
    val lastIt = last.iterator.buffered
    var dropped = Integral[V].zero
    Runs(
      first ++ lastIt.dropWhile(t => {
        val (_, count) = t
        val drop = dropped < num
        dropped += count
        drop
      })
    )
  }

  override def toString: String = {
    val numSampled = numFirst + numLast
    val numSkipped = n - numSampled
    if (numSkipped > 0) {
      s"$first, …, $last"
    } else {
      removeOverlap(-numSkipped, first, last).toString
    }
  }
}

case class Stats[K, V: Integral](n: V,
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

object Stats {

  def getRunPercentiles[K: Numeric, V: Integral](values: Seq[(K, V)],
                                                 ps: Seq[(Double, Double)]): Vector[(Double, Double)] =
    getRunPercentiles(
      values
        .map(t => t._1.toDouble() -> t._2.toDouble())
        .iterator
        .buffered,
      ps
        .iterator
        .buffered
    ).toVector

  def getRunPercentiles[K: Numeric](values: BufferedIterator[(Double, Double)],
                                    ps: BufferedIterator[(Double, Double)]): Iterator[(Double, Double)] =
    new Iterator[(Double, Double)] {

      var elemsPast = 0.0
      var curK: Option[Double] = None

      override def hasNext: Boolean = ps.hasNext

      override def next(): (Double, Double) = {
        val (percentile, idx) = ps.next()
        while(elemsPast < idx) {
          val (k, v) = values.next()
          curK = Some(k)
          elemsPast += v
        }

        val distancePast = elemsPast - idx

        percentile ->
          (
            if (distancePast < 1)
              curK.get * distancePast + values.head._1 * (1 - distancePast)
            else
              curK.get
          )
      }
    }

  def histPercentiles[K: Numeric, V: Integral](N: V, values: IndexedSeq[(K, V)]): Vector[(Double, Double)] = {
    val n = N - 1
    val denominators: Iterator[Int] = Iterator(2, 4, 10, 20, 100, 1000, 10000)

    val nd = n.toDouble
    val percentileIdxs =
      denominators.takeWhile(_ <= n).flatMap(d => {
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

  def percentiles[T: Numeric](values: IndexedSeq[T]): Vector[(Double, Double)] = {
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

  def getMedian[T: Numeric](sorted: Vector[T]): Double = {
    val n = sorted.length
    if (n == 0)
      -1
    else if (n % 2 == 0)
      (sorted(n / 2 - 1) + sorted(n / 2)).toDouble() / 2.0
    else
      sorted(n / 2).toDouble()
  }

  def fromHist[K: Numeric: Ordering, V: Integral](v: Iterable[(K, V)],
                                                  numToSample: Int = 10,
                                                  onlySampleSorted: Boolean = false): Stats[K, V] = {
    val vBuilder = Vector.newBuilder[(K, V)]
    var alreadySorted = true
    var prevOpt: Option[K] = None
    var n = Integral[V].zero
    val hist = mutable.HashMap[K, V]()
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

    val values = vBuilder.result()

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

  def runLengthEncodeWithSum[K: Numeric](it: Iterator[K],
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
