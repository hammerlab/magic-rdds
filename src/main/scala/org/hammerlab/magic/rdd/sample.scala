package org.hammerlab.magic.rdd

import hammerlab.lines.limit._
import magic_rdds.size._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait sample {
  implicit class SampleOps[T: ClassTag](rdd: RDD[T])
    extends Serializable {
    def sample(implicit limit: Limit): Array[T] =
      sample(rdd.size)

    def sample(size: Long)(implicit limit: Limit): Array[T] =
      limit match {
        case Limit(ss)
          if size > ss ⇒
          rdd.take(ss)
        case _  if size == 0 ⇒
          Array()
        case _ ⇒
          rdd.collect()
      }
  }
}
