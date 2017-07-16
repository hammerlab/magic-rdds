package org.hammerlab.io

import java.io.{ Closeable, EOFException, IOException, InputStream }
import java.nio.channels.ReadableByteChannel
import java.nio.{ ByteBuffer, ByteOrder, channels }

import org.apache.hadoop.fs.Seekable

/**
 * Readable, "skippable" common interface over [[InputStream]]s, [[Iterator[Byte]]]s, and
 * [[channels.SeekableByteChannel]]s.
 *
 * When wrapping [[channels.SeekableByteChannel]]s or [[Seekable]]s, exposes [[SeekableByteChannel.seek]] as well.
 */
trait ByteChannel
  extends InputStream
    with ReadableByteChannel {

  protected var _position = 0L

  /**
   * Read as many bytes into `dst` as it has remaining, throw an [[IOException]] if too few bytes exist or are read.
   */
  def readFully(dst: ByteBuffer): Unit = {
    val numToRead = dst.remaining()

    var numRead = read(dst)
    if (numRead == -1)
      throw new EOFException

    if (numRead < numToRead) {
      val secondRead = read(dst)
      if (secondRead == -1)
        throw new EOFException

      numRead += secondRead

      if (numRead < numToRead)
        throw new IOException(
          s"Got $numRead (${numRead - secondRead} then $secondRead) of $numToRead bytes in 2 attempts from position ${position() - numRead}"
        )
    }
  }
  def readFully(bytes: Array[Byte]): Unit = readFully(bytes, 0, bytes.length)
  def readFully(bytes: Array[Byte], offset: Int, numToRead: Int): Unit = {
    var numRead = read(bytes, offset, numToRead)
    if (numRead == -1)
      throw new EOFException

    if (numRead < numToRead) {
      val secondRead =
        read(
          bytes,
          offset + numRead,
          numToRead - numRead
        )

      if (secondRead == -1)
        throw new EOFException

      numRead += secondRead
    }

    if (numRead < numToRead)
      throw new IOException(
        s"Got $numRead of $numToRead bytes in 2 attempts from position ${position() - numRead}"
      )
  }

  override final def read: Int =
    _read() match {
      case -1 ⇒ -1
      case b ⇒
        _position += 1
        b
    }

  override final def read(b: Array[Byte]): Int = read(b, 0, b.length)
  override final def read(b: Array[Byte], off: Int, len: Int): Int =
    _read(b, off, len) match {
      case -1 ⇒ -1
      case n ⇒
        _position += n
        n
    }

  override final def read(dst: ByteBuffer): Int =
    _read(dst) match {
      case -1 ⇒ -1
      case n ⇒
        _position += n
        n
    }

  protected def _read(): Int
  protected def _read(b: Array[Byte], off: Int, len: Int): Int
  protected def _read(dst: ByteBuffer): Int

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
    readFully(b4)
    b4.getInt(0)
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

  private var _closed = false
  override final def close(): Unit = {
    _closed = true
    _close()
  }

  protected def _close(): Unit = {}

  override def isOpen: Boolean = !_closed
}

object ByteChannel {

  implicit class IteratorByteChannel(it: Iterator[Byte])
    extends ArrayByteChannel {

    override def _read(): Int =
      if (it.hasNext)
        it.next & 0xff
      else
        -1

    override def _skip(n: Int): Unit = {
      var idx = 0
      while (idx < n) {
        it.next
        idx += 1
      }
    }

    override protected def _close(): Unit =
      it match {
        case c: Closeable ⇒
          c.close()
      }
  }

  implicit class InputStreamByteChannel(is: InputStream)
    extends ArrayByteChannel {

    override def _read(): Int = is.read()
    override def _read(b: Array[Byte], off: Int, len: Int): Int = is.read(b, off, len)

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

    override protected def _close(): Unit =
      is.close()
  }
}

trait BufferByteChannel
  extends ByteChannel {

  val b1 = Buffer(1)
  override protected def _read(): Int = {
    b1.clear()
    _read(b1) match {
      case x if x <= 0 ⇒ x
      case 1 ⇒ b1.get(0) & 0xff
      case n ⇒
        throw new IOException(
          s"Read $n bytes when only asked for 1 at position ${position() - n}"
        )
    }
  }

  override protected def _read(b: Array[Byte], off: Int, len: Int): Int =
    _read(Buffer(b, off, len))
}

trait ArrayByteChannel
  extends ByteChannel {
  /**
   * Implement reading into a [[ByteBuffer]] in terms of reads into an [[Array]] of [[Byte]]s.
   */
  override protected def _read(dst: ByteBuffer): Int =
    _read(
      dst.array(),
      dst.position(),
      dst.remaining()
    ) match {
      case -1 ⇒ -1
      case n ⇒
        dst.position(dst.position() + n)
        n
    }

  override protected def _read(bytes: Array[Byte], off: Int, len: Int): Int = {
    val end = off + len
    if (end > bytes.length)
      throw new IllegalArgumentException

    var idx = off
    while (idx < end) {
      val b = _read()
      if (b == -1)
        return (
          if (idx == off)
            -1
          else
            idx - off
          )

      bytes(idx) = b.toByte
      idx += 1
    }

    idx - off
  }
}
