package org.hammerlab.magic.rdd

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.rdd.RDD
import org.hammerlab.iterator.{RangeAccruingIterator, RunLengthIterator}
import org.hammerlab.iterator.RunLengthIterator._
import org.hammerlab.magic.rdd.sliding.BorrowElemsRDD._

import scala.collection.SortedSet
import scala.reflect.ClassTag

/**
 * Helper for run-length encoding an [[RDD]].
 */
class RunLengthRDD[T: ClassTag](rdd: RDD[T]) {
  lazy val runLengthEncode: RDD[(T, Long)] = {
    val runLengthPartitions =
      rdd.mapPartitions(_.runLengthEncode())

    val oneOrFewerElementPartitions =
      SortedSet(
        runLengthPartitions
          .mapPartitionsWithIndex((idx, it) ⇒
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
        range ← new RangeAccruingIterator(oneOrFewerElementPartitions.iterator)
        sendTo = math.max(0, range.start - 1)
        i ← range
      } yield
        (i + 1) → sendTo
      )
      .toMap

    runLengthPartitions
      .shiftLeft(1, partitionOverrides, allowIncompletePartitions = true)
      .mapPartitions(it ⇒ RunLengthIterator.reencode(it.map(t ⇒ t._1 → t._2.toLong).buffered))
  }
}

object RunLengthRDD {

  def register(kryo: Kryo): Unit = {
    kryo.register(classOf[Array[Int]])
  }

  implicit def rddToRunLengthRDD[T: ClassTag](rdd: RDD[T]): RunLengthRDD[T] = new RunLengthRDD(rdd)
}
