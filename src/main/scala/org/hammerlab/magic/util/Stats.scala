package org.hammerlab.magic.util

import org.hammerlab.magic.iterator.{OptionIterator, RunLengthIterator}

import spire.math.Numeric
import spire.implicits._

import scala.collection.mutable.ArrayBuffer

case class Stats[T](n: Int,
                    firstElems: Seq[(T, Int)],
                    numFirstElems: Int,
                    lastElems: Seq[(T, Int)],
                    numLastElems: Int,
                    leastElems: Seq[(T, Int)],
                    numLeastElems: Int,
                    greatestElems: Seq[(T, Int)],
                    numGreatestElems: Int,
                    percentiles: Seq[(Double, Double)]) {

  def elemsToStr(elems: Seq[(T, Int)]): String =
    (for ((elem, count) <- elems) yield
      if (count == 1)
        elem.toString
      else
        s"$elem($count)"
    ).mkString(", ")

  def removeOverlap(num: Int, first: Seq[(T, Int)], last: Seq[(T, Int)]): Seq[(T, Int)] = {
    val lastIt = last.iterator.buffered
    var dropped = 0
    first ++ lastIt.dropWhile(t => {
      val (_, count) = t
      val drop = dropped < num
      dropped += count
      drop
    })
  }

  def prettyDouble(d: Double): String = {
    if (math.floor(d).toInt == math.ceil(d).toInt)
      d.toInt.toString
    else
      "%.1f".format(d)
  }

  def rangeToStr(first: Seq[(T, Int)],
                 numFirst: Int,
                 last: Seq[(T, Int)],
                 numLast: Int): String = {
    val numSampled = numFirst + numLast
    val numSkipped = n - numSampled
    if (numSkipped > 0) {
      s"${elemsToStr(first)}, …, ${elemsToStr(last)}"
    } else {
      elemsToStr(removeOverlap(-numSkipped, first, last))
    }
  }

  override def toString: String = {
    if (n == 0)
      "(empty)"
    else {
      val strings = ArrayBuffer[String]()

      if (firstElems.nonEmpty) {
        strings += s"elems:\t${rangeToStr(firstElems, numFirstElems, lastElems, numLastElems)}"
        strings += s"sorted:\t${rangeToStr(leastElems, numLeastElems, greatestElems, numGreatestElems)}"
      }

      strings ++= percentiles.map(t => s"${prettyDouble(t._1)}:\t${prettyDouble(t._2)}")

      strings.mkString("\n")
    }
  }
}

object Stats {

  def percentiles[T: Numeric](values: IndexedSeq[T]): Vector[(Double, Double)] = {
    val n = values.length - 1
    val denominators: Iterator[Int] = {
      lazy val pow10s: Stream[Int] = 100 #:: pow10s.map(_ * 10)
      Iterator(2, 4, 10, 20) ++ pow10s.iterator
    }

    val nd = n.toDouble
    denominators.takeWhile(_ <= n).flatMap(d => {
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

      val loPercentile = 100.0 / d
      val hiPercentile = 100.0 - loPercentile

      if (d == 2)
        Iterator(loPercentile -> lo)
      else
        Iterator(loPercentile -> lo, hiPercentile -> hi)
    }).toVector.sortBy(_._1)
  }

  def apply[T: Numeric: Ordering](v: Iterable[T], numToSample: Int = 10): Stats[T] = {

    val values = v.toVector
    val sorted = values.sorted

    val n = values.length

    // Count occurrences of the first N distinct values.
    val (firstElems, numFirstElems) = runLengthEncodeWithSum(values.iterator, numToSample)

    // Count occurrences of the last N distinct values.
    val (lastElems, numLastElems) = runLengthEncodeWithSum(values.reverseIterator, numToSample, reverse = true)

    // Count occurrences of the least N distinct values.
    val (leastElems, numLeastElems) = runLengthEncodeWithSum(sorted.iterator, numToSample)

    // Count occurrences of the greatest N distinct values.
    val (greatestElems, numGreatestElems) = runLengthEncodeWithSum(sorted.reverseIterator, numToSample, reverse = true)

    // Compute some nice, round percentile values, excluding ones that are already includes in the "first N" and
    // "last N".

    val firstNPercentile = numLeastElems * 100.0 / (n - 1)
    val lastNPercentile =  100 * (1 - numGreatestElems.toDouble / (n - 1))

    Stats(
      n,
      firstElems, numFirstElems,
      lastElems, numLastElems,
      leastElems, numLeastElems,
      greatestElems, numGreatestElems,
      percentiles(sorted)
    )
  }

  def runLengthEncodeWithSum[T](it: Iterator[T], N: Int, reverse: Boolean = false): (Seq[(T, Int)], Int) = {
    var sum = 0
    var i = 0
    val runs = ArrayBuffer[(T, Int)]()
    val runLengthIterator = RunLengthIterator(it)
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

//  def runLengthEncode[NT: Numeric](it: Iterator[NT], N: Int): Seq[(NT, Int)] = RunLengthIterator(it).take(N).toArray[(NT, Int)]
}
