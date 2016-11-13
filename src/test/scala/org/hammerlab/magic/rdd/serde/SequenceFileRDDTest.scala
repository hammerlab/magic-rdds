package org.hammerlab.magic.rdd.serde

import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.serde.SequenceFileSerializableRDD._
import org.hammerlab.magic.test.rdd.VerifyRDDSerde

import scala.reflect.ClassTag

/**
 * Base-trait for tests of correctness and on-disk size of
 * [[org.hammerlab.magic.rdd.serde.SequenceFileSerializableRDD]], with an optional compression codec.
 */
trait SequenceFileRDDTest
  extends VerifyRDDSerde {

  def codec: Class[_ <: CompressionCodec] = null

  private def codecOpt = Option(codec)

  def serializeRDD[T: ClassTag](rdd: RDD[T], path: String): RDD[T] =
    rdd.saveSequenceFile(path, codecOpt)

  def deserializeRDD[T: ClassTag](path: String): RDD[T] =
    sc.fromSequenceFile[T](path, splittable = false)
}
