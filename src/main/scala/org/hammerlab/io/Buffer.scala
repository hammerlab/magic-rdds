package org.hammerlab.io

import java.nio.ByteOrder.LITTLE_ENDIAN
import java.nio.{ ByteBuffer, ByteOrder }

object Buffer {
  /** Shorthand for [[ByteBuffer]] creation */
  def apply(capacity: Int)(
      implicit order: ByteOrder = LITTLE_ENDIAN
  ): ByteBuffer =
    ByteBuffer
      .allocate(capacity)
      .order(LITTLE_ENDIAN)

  def apply(bytes: Array[Byte]): ByteBuffer = ByteBuffer.wrap(bytes)

  def apply(bytes: Array[Byte], offset: Int, length: Int): ByteBuffer = {
    val buffer = apply(bytes)
    buffer.position(offset)
    buffer.limit(offset + length)
    buffer
  }
}
