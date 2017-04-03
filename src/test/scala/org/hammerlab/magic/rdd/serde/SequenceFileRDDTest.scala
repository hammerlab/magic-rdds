package org.hammerlab.magic.rdd.serde

import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.serde.SequenceFileSerializableRDD._
import org.hammerlab.spark.test.rdd.RDDSerialization
import org.hammerlab.paths.Path

import scala.reflect.ClassTag

/**
 * Base-trait for tests of correctness and on-disk size of
 * [[org.hammerlab.magic.rdd.serde.SequenceFileSerializableRDD]], with an optional compression codec.
 */
trait SequenceFileRDDTest
  extends RDDSerialization {

  def codec: Class[_ <: CompressionCodec] = null

  private def codecOpt = Option(codec)

  def serializeRDD[T: ClassTag](rdd: RDD[T], path: Path): RDD[T] =
    rdd.saveSequenceFile(path.toString, codecOpt)

  def deserializeRDD[T: ClassTag](path: Path): RDD[T] =
    sc.fromSequenceFile[T](path.toString, splittable = false)
}
