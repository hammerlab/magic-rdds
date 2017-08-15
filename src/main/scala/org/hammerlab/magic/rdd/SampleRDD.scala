package org.hammerlab.magic.rdd

import org.apache.spark.rdd.RDD
import org.hammerlab.io.SampleSize
import org.hammerlab.magic.rdd.size._

import scala.reflect.ClassTag

case class SampleRDD[T: ClassTag](@transient rdd: RDD[T]) {
  def sample(implicit sampleSize: SampleSize): Array[T] =
    sample(rdd.size)

  def sample(size: Long)(implicit sampleSize: SampleSize): Array[T] =
    sampleSize match {
      case SampleSize(Some(ss))
        if size > ss ⇒
        rdd.take(ss)
      case _  if size == 0 ⇒
        Array()
      case _ ⇒
        rdd.collect()
    }
}

object SampleRDD {
  implicit def makeSampleRDD[T: ClassTag](rdd: RDD[T]): SampleRDD[T] =
    SampleRDD(rdd)
}
