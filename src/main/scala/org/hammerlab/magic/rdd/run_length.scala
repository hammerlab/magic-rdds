package org.hammerlab.magic.rdd

import hammerlab.iterator._
import magic_rdds._
import org.apache.spark.rdd.RDD
import org.hammerlab.iterator.group.Cmp
import org.hammerlab.kryo._

import scala.math.max
import scala.reflect.ClassTag

trait run_length {
  /**
   * Helper for run-length encoding an [[RDD]].
   */
  implicit class RunLengthOps[T: ClassTag : Cmp](rdd: RDD[T])
    extends Serializable {
    lazy val runLengthEncode: RDD[(T, Long)] = {
      val runLengthPartitions =
        rdd.mapPartitions(_.runLengthEncode)

      val oneOrFewerElementPartitions =
        runLengthPartitions
          .filterPartitionIdxs(_.take(2).size < 2)

      val partitionOverrides =
        (for {
          range ← oneOrFewerElementPartitions.contiguousRanges
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
          _
            .map(t ⇒ t._1 → t._2.toLong)
            .runLengthReencode
        )
    }
  }
}

object run_length
  extends spark.Registrar(
    arr[Int]
  )
