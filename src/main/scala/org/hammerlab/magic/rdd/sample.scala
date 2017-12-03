package org.hammerlab.magic.rdd

import hammerlab.print.SampleSize
import magic_rdds.size._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait sample {
  implicit class SampleOps[T: ClassTag](rdd: RDD[T]) extends Serializable {
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
}
