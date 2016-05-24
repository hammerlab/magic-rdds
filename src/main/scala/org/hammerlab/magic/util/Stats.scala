package org.hammerlab.magic.util

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.hammerlab.magic.iterator.RunLengthIterator
import org.hammerlab.magic.util.Stats.Hist
import spire.implicits._
import spire.math.Numeric

import scala.collection.mutable.ArrayBuffer

case class Stats(firstElems: Hist,
                 lastElems: Hist,
                 leastElems: Hist,
                 greatestElems: Hist,
                 percentiles: Seq[(Double, Int)]) {

  def histToStr(h: Hist): String = h.map(t => if (t._2 == 1) t._1.toString else s"${t._1}(${t._2})").mkString(", ")

  override def toString: String = {
    val strings = ArrayBuffer[String]()

    strings +=  s"first:\t${histToStr(firstElems)}"
    strings +=  s"last:\t${histToStr(lastElems)}"

    strings +=  s"min:\t${histToStr(leastElems)}"
    strings ++= percentiles.map(t => s"${t._1}:\t${t._2}")
    strings +=  s"max:\t${histToStr(greatestElems)}"

    strings.mkString("\n")
  }
}

object Stats {
  val roundPercentiles = Array[Double](50, 25, 5, 1, 0.1, 2, 10)

  type Hist = Seq[(Int, Int)]

  def apply[NT: Numeric](values: Iterable[NT], N: Int = 10, P: Int = 9): Stats = {
    val stats = new DescriptiveStatistics()
    values.foreach(value => stats.addValue(value.toDouble()))
    Stats(stats, N, P)
  }

  private def apply(stats: DescriptiveStatistics, N: Int, P: Int): Stats = {
    val values = stats.getValues.map(_.toInt)

    // Count occurrences of the first N distinct values.
    val firstElems = runLengthEncode(values.iterator, N)

    // Count occurrences of the last N distinct values.
    val lastElems = runLengthEncode(values.reverseIterator, N).reverse

    val sorted = stats.getSortedValues.map(_.toInt)

    // Count occurrences of the least N distinct values.
    val (leastElems, leastN) = runLengthEncodeWithSum(sorted.iterator, N)

    // Count occurrences of the greatest N distinct values.
    val (greatestElems, greatestN) = runLengthEncodeWithSum(sorted.reverseIterator, N)

    val n = stats.getN.toInt

    // Compute some nice, round percentile values, excluding ones that are already includes in the "first N" and
    // "last N".

    val firstNPercentile = leastN * 100.0 / (n - 1)
    val lastNPercentile =  100 * (1 - greatestN.toDouble / (n - 1))

    var numPercentilesToAdd = P
    val percentiles =
      (for {
        percentileTier <- Stats.roundPercentiles
        if numPercentilesToAdd > 0
        percentile <- Set(percentileTier, 100 - percentileTier)
        if firstNPercentile < percentile && percentile < lastNPercentile
      } yield {
        numPercentilesToAdd -= 1
        percentile -> stats.getPercentile(percentile).toInt
      }).sortBy(_._1)

    Stats(firstElems, lastElems, leastElems, greatestElems, percentiles)
  }

  def runLengthEncodeWithSum(it: Iterator[Int], N: Int): (Hist, Int) = {
    val rle = runLengthEncode(it, N)
    (rle, rle.map(_._2).sum)
  }

  def runLengthEncode(it: Iterator[Int], N: Int): Hist = RunLengthIterator[Int](it).take(N).toArray[(Int, Int)]
}
