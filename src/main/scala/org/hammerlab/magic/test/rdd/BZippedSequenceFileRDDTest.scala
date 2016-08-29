package org.hammerlab.magic.test.rdd

import org.apache.hadoop.io.compress.{BZip2Codec, CompressionCodec}

/**
 * Mix-in that causes sequence-files to be written with BZip2 compression.
 */
trait BZippedSequenceFileRDDTest
  extends SequenceFileRDDTest {

  override def codec: Class[_ <: CompressionCodec] = classOf[BZip2Codec]
}
