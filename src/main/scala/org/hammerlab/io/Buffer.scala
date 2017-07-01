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
}
