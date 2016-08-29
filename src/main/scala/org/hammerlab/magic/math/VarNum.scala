package org.hammerlab.magic.math

import java.io.{OutputStream, InputStream}

/**
 * Serialization wrapper for [[Long]]s which burns one bit per byte indicating whether any more bytes follow.
 *
 * Can utilize less serialized space than naively writing 8-byte [[Long]]s in datasets where absolute values tend to be
 * less than 2^48 more often than they are ≥ 2^55.
 *
 * [[Long]]'s absolute values correspond to the following number of serialized bytes:
 *
 *   [   0,  2^6): 1 byte
 *   [ 2^6, 2^13): 2 bytes
 *   [2^13, 2^20): 3 bytes
 *   [2^20, 2^27): 4 bytes
 *   [2^27, 2^34): 5 bytes
 *   [2^34, 2^41): 6 bytes
 *   [2^41, 2^48): 7 bytes
 *   [2^48, 2^55): 8 bytes
 *   [2^55, 2^63): 9 bytes
 *
 * The first byte, in addition to its most significant bit indicating whether any more bites follow, uses its
 * second-most-significant bit to represent the sign of the [[Long]].
 */
object VarNum {
  def write(output: OutputStream, l: Long): Unit = {
    var n = l
    var more = true
    var total = 0
    while (more) {
      if (total == 56) {
        output.write(n.toByte)
        more = false
      } else {
        val b =
          if (total == 0) {
            val byte =
              if (n < 0) {
                n = -n
                (n & 0x3F).toByte | 0x40
              } else {
                (n & 0x3F).toByte
              }

            n = n >> 6
            byte
          } else {
            val byte = (n & 0x7F).toByte
            n = n >> 7
            byte
          }

        total += 7
        more = n > 0
        output.write(b | (if (more) 0x80 else 0).toByte)
      }
    }
  }

  def read(input: InputStream): Long = {
    var l = 0L
    var bits = 0
    val readBytes = Array[Byte](0)
    var negate = false
    while (bits < 63) {
      input.read(readBytes)
      val b = readBytes(0)
      if (bits == 55) {
        l += ((b & 0xffL) << bits)
        bits += 8
      } else {
        if (bits == 0) {
          negate = (b & 0x40) > 0
          l += (b & 0x3FL)
          bits += 6
        } else {
          l += (b & 0x7FL) << bits
          bits += 7
        }

        if ((b & 0x80) == 0) {
          bits = 63
        }
      }
    }
    if (negate)
      -l
    else
      l
  }
}
