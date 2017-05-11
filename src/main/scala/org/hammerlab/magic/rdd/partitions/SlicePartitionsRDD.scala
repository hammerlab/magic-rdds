package org.hammerlab.magic.rdd.partitions

import org.apache.spark.rdd.{ PartitionPruningRDD, RDD }

import scala.reflect.ClassTag

case class SlicePartitionsRDD[T: ClassTag](prev: RDD[T],
                                           start: Int,
                                           end: Int)
  extends PartitionPruningRDD[T](
    prev,
    idx ⇒ start <= idx && idx < end
  )
