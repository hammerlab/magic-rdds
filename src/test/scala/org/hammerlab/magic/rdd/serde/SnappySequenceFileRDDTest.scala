package org.hammerlab.magic.rdd.serde

import org.apache.hadoop.io.compress.{CompressionCodec, SnappyCodec}

/**
 * Mix-in that causes sequence-files to be written with Snappy compression.
 */
trait SnappySequenceFileRDDTest
  extends SequenceFileRDDTest {

  override def codec: Class[_ <: CompressionCodec] = classOf[SnappyCodec]
}
