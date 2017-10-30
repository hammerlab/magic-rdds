package org.hammerlab.magic.rdd

import magic_rdds.sliding._
import org.apache.spark.rdd.RDD
import org.hammerlab.iterator.RangeAccruingIterator
import org.hammerlab.iterator.RunLengthIterator._
import org.hammerlab.kryo.spark.Registrar
import org.hammerlab.magic.rdd.partitions.FilterPartitionIdxs._

import scala.math.max
import scala.reflect.ClassTag

/**
 * Helper for run-length encoding an [[RDD]].
 */
case class RunLengthRDD[T: ClassTag](rdd: RDD[T]) {
  lazy val runLengthEncode: RDD[(T, Long)] = {
    val runLengthPartitions =
      rdd.mapPartitions(_.runLengthEncode())

    val oneOrFewerElementPartitions =
      runLengthPartitions
        .filterPartitionIdxs(_.take(2).size < 2)

    val partitionOverrides =
      (for {
        range ← new RangeAccruingIterator(oneOrFewerElementPartitions.iterator)
        sendTo = max(0, range.start - 1)
        i ← range
      } yield
        (i + 1) → sendTo
      )
      .toMap

    runLengthPartitions
      .shiftLeft(
        1,
        partitionOverrides
      )
      .mapPartitions(
        it ⇒
          reencode(
            it
              .map(t ⇒ t._1 → t._2.toLong)
          )
      )
  }
}

object RunLengthRDD
  extends Registrar(
    classOf[Array[Int]]
  ) {
  implicit def ooRunLengthRDD[T: ClassTag](rdd: RDD[T]): RunLengthRDD[T] = RunLengthRDD(rdd)
}
