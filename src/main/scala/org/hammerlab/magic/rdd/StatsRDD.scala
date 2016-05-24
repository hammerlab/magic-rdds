//package org.hammerlab.magic.rdd
//
//import org.apache.spark.rdd.RDD
//
//import scala.reflect.ClassTag
//
//case class Stats(numPartitions: Int, partitionLengths: Array[Int])
//
//class StatsRDD[T: ClassTag](@transient val rdd: RDD[T]) extends Serializable {
//  def stats(): Stats = {
//    Stats(rdd.getNumPartitions, rdd.mapPartitions(it => Iterator(it.size)).collect)
//  }
//}
//
//object StatsRDD {
//  implicit def toStatsRDD[T: ClassTag](rdd: RDD[T]): StatsRDD[T] = new StatsRDD(rdd)
//}
