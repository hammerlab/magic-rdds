package org.hammerlab.magic.rdd

import org.apache.spark.rdd.RDD
import org.hammerlab.magic.iterator.{RangeAccruingIterator, RunLengthIterator}
import BorrowElemsRDD._

import scala.collection.SortedSet
import scala.collection.immutable.Range
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

case class CollapsePartitions[T](start: Int, end: Int, elem: T, count: Int)

class RunLengthRDD[T: ClassTag](rdd: RDD[T]) {
  lazy val runLengthEncode = {
    val runLengthPartitions =
      rdd
        .mapPartitions(it => RunLengthIterator(it))

    val oneOrFewerElementPartitions =
      SortedSet(
        runLengthPartitions
          .mapPartitionsWithIndex((idx, it) =>
            if (it.hasNext) {
              val n = it.next()
              if (it.hasNext)
                Iterator()
              else
                Iterator(idx)
            } else
              Iterator(idx)
          )
          .collect(): _*
      )

    val partitionOverrides =
      (for {
        range <- new RangeAccruingIterator(oneOrFewerElementPartitions.iterator)
        sendTo = math.max(0, range.start - 1)
        i <- range
      } yield {
        (i + 1) -> sendTo
      }).toMap

    runLengthPartitions
      .shift(1, partitionOverrides, allowIncompletePartitions = true)
      .mapPartitions(it => RunLengthIterator.reencode(it.buffered))

  }
}

object RunLengthRDD {
  implicit def rddToRunLengthRDD[T: ClassTag](rdd: RDD[T]): RunLengthRDD[T] = new RunLengthRDD(rdd)
}
