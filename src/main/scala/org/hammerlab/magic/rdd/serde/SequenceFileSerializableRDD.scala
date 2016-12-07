package org.hammerlab.magic.rdd.serde

import java.nio.ByteBuffer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.{BZip2Codec, CompressionCodec}
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkEnv}
import org.hammerlab.magic.hadoop.UnsplittableSequenceFileInputFormat

import scala.reflect.ClassTag

/**
 * Helpers for saving [[RDD]]s as Hadoop sequence-files, optionally compressing them.
 */
class SequenceFileSerializableRDD[T: ClassTag](@transient val rdd: RDD[T]) extends Serializable {
  def saveCompressed(path: String): RDD[T] =
    saveSequenceFile(
      path,
      Some(classOf[BZip2Codec])
    )

  def saveCompressed(path: String,
                     codec: Class[_ <: CompressionCodec]): RDD[T] =
    saveSequenceFile(
      path,
      Some(codec)
    )

  def saveCompressed(path: Path): RDD[T] =
    saveSequenceFile(
      path.toString,
      Some(classOf[BZip2Codec])
    )

  def saveCompressed(path: Path, codec: Class[_ <: CompressionCodec]): RDD[T] =
    saveSequenceFile(
      path.toString,
      Some(codec)
    )

  def saveSequenceFile(path: String, codec: Class[_ <: CompressionCodec]): RDD[T] =
    saveSequenceFile(
      path,
      Some(codec)
    )

  def saveSequenceFile(path: String, compress: Boolean): RDD[T] =
    saveSequenceFile(
      path,
      if (compress)
        Some(classOf[BZip2Codec])
      else
        None
    )

  def saveSequenceFile(path: String,
                       codec: Option[Class[_ <: CompressionCodec]] = None): RDD[T] = {
    rdd
      .mapPartitions(iter => {
        val serializer = SparkEnv.get.serializer.newInstance()
        iter.map(x =>
          (
            NullWritable.get(),
            new BytesWritable(serializer.serialize(x).array())
          )
        )
      })
      .saveAsSequenceFile(path, codec)

    rdd
  }
}

object SequenceFileSerializableRDD {
  implicit def toSerializableRDD[T: ClassTag](rdd: RDD[T]): SequenceFileSerializableRDD[T] = new SequenceFileSerializableRDD(rdd)
  implicit def toSerdeSparkContext(sc: SparkContext): SequenceFileSparkContext = new SequenceFileSparkContext(sc)
}

class SequenceFileSparkContext(val sc: SparkContext) {

  def unsplittableSequenceFile[K, V](path: String,
                                     keyClass: Class[K],
                                     valueClass: Class[V],
                                     minPartitions: Int
                                    ): RDD[(K, V)] =
    sc.hadoopFile(
      path,
      classOf[UnsplittableSequenceFileInputFormat[K, V]],
      keyClass,
      valueClass,
      minPartitions
    )

  def fromSequenceFile[T: ClassTag](path: Path): RDD[T] =
    fromSequenceFile(path, splittable = true)

  def fromSequenceFile[T: ClassTag](path: Path,
                                    splittable: Boolean): RDD[T] =
    fromSequenceFile(path.toString, splittable)

  def fromSequenceFile[T: ClassTag](path: String,
                                    splittable: Boolean = true): RDD[T] = {
    val rdd =
      if (splittable)
        sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], 1)
      else
        unsplittableSequenceFile(path, classOf[NullWritable], classOf[BytesWritable], 1)

    rdd.mapPartitions[T](iter => {
      val serializer = SparkEnv.get.serializer.newInstance()
      iter.map(x =>
        serializer.deserialize(ByteBuffer.wrap(x._2.getBytes))
      )
    })
  }
}

