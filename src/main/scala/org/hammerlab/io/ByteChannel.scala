package org.hammerlab.io

import java.io.{ Closeable, EOFException, IOException, InputStream }
import java.nio.{ ByteBuffer, ByteOrder, channels }

import org.apache.hadoop.fs.Seekable

/**
 * Readable, "skippable" common interface over [[InputStream]]s, [[Iterator[Byte]]]s, and
 * [[channels.SeekableByteChannel]]s.
 *
 * When wrapping [[channels.SeekableByteChannel]]s or [[Seekable]]s, exposes [[SeekableByteChannel.seek]] as well.
 */
trait ByteChannel
  extends InputStream {

  protected var _position = 0L

  /**
   * Read as many bytes into `dst` as it has remaining, throw an [[IOException]] if too few bytes exist or are read.
   */
  final def read(dst: ByteBuffer): Unit = {
    val n = dst.remaining()
    _read(dst)
    _position += n
  }

  val b1 = Buffer(1)
  override def read(): Int = {
    b1.clear()
    read(b1)
    b1.get(0) & 0xff
  }


  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    read(ByteBuffer.wrap(b), off, len)
    len
  }

  /**
   * Convenience method for reading a string of known length
   */
  def readString(length: Int, includesNull: Boolean = true): String = {
    val buffer = Buffer(length)
    read(buffer)
    buffer
      .array()
      .slice(
        0,
        if (includesNull)
          length - 1
        else
          length
      )
      .map(_.toChar)
      .mkString("")
  }

  lazy val b4 = Buffer(4)

  def order(order: ByteOrder): Unit =
    b4.order(order)

  def getInt: Int = {
    b4.position(0)
    read(b4)
    b4.getInt(0)
  }

  protected def _read(dst: ByteBuffer): Unit

  def read(dst: ByteBuffer, offset: Int, length: Int): Unit = {
    dst.position(offset)
    val prevLimit = dst.limit()
    dst.limit(offset + length)
    read(dst)
    dst.limit(prevLimit)
  }

  /**
   * Skip `n` bytes, throw [[IOException]] if unable to
   */
  final def skip(n: Int): Unit = {
    _skip(n)
    _position += n
  }

  protected def _skip(n: Int): Unit

  def position(): Long = _position
}

object ByteChannel {

  implicit class IteratorByteChannel(it: Iterator[Byte])
    extends ByteChannel {

    override def read(): Int =
      if (it.hasNext)
        it.next & 0xff
      else
        -1

    override def _read(dst: ByteBuffer): Unit = {
      var idx = 0
      val size = dst.limit() - dst.position()
      while (idx < size && it.hasNext) {
        dst.put(it.next)
        idx += 1
      }
      if (idx < size)
        throw new IOException(
          s"Only found $idx of $size bytes at position ${position()}"
        )
    }

    override def _skip(n: Int): Unit =
      it.drop(n)

    override def close(): Unit =
      it match {
        case c: Closeable ⇒
          c.close()
      }
  }

  implicit class InputStreamByteChannel(is: InputStream)
    extends ByteChannel {

    override def read(): Int = is.read()

    override def _read(dst: ByteBuffer): Unit = {
      val bytesToRead = dst.remaining()
      var bytesRead =
        is.read(
          dst.array(),
          dst.position(),
          dst.remaining()
        )

      if (bytesRead == -1)
        throw new EOFException

      val nextBytesRead =
        if (bytesRead < bytesToRead) {
          val moreBytesRead =
            is.read(
              dst.array(),
              dst.position() + bytesRead,
              dst.remaining() - bytesRead
            )

          if (moreBytesRead == -1)
            throw new EOFException

          bytesRead += moreBytesRead

          moreBytesRead
        } else
          0

      if (bytesRead < bytesToRead) {
        throw new IOException(
          s"Only read $bytesRead (${bytesRead - nextBytesRead} then $nextBytesRead) of $bytesToRead bytes from position ${position()}"
        )
      }

      dst.position(dst.position() + bytesRead)
    }

    override def _skip(n: Int): Unit = {
      var remaining = n.toLong
      while (remaining > 0) {
        val skipped = is.skip(remaining)
        if (skipped <= 0)
          throw new IOException(
            s"Only skipped $skipped of $remaining, total $n (${is.available()})"
          )
        remaining -= skipped
      }
    }

    override def close(): Unit =
      is.close()
  }
}
