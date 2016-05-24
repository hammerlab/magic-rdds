package org.hammerlab.magic.rdd

import org.apache.spark.rdd.RDD
import org.hammerlab.magic.iterator.RunLengthIterator
import BorrowElemsRDD._

import scala.reflect.ClassTag

class RunLengthRDD[T: ClassTag](rdd: RDD[T]) {
  lazy val runLengthEncode =
    rdd
      .mapPartitions(it => RunLengthIterator(it))
      .shift(1)
      .mapPartitions(it => RunLengthIterator.reencode(it.buffered))
}

object RunLengthRDD {
  implicit def rddToRunLengthRDD[T: ClassTag](rdd: RDD[T]): RunLengthRDD[T] = new RunLengthRDD(rdd)
}
