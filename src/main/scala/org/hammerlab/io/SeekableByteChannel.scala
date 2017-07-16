package org.hammerlab.io

import java.nio.file.Files.newByteChannel
import java.nio.{ ByteBuffer, channels }

import org.hammerlab.paths

trait SeekableByteChannel
  extends ByteChannel {

  def seek(newPos: Long): Unit = {
    _position = newPos
    _seek(newPos)
  }

  override def _skip(n: Int): Unit = _seek(position() + n)

  def size: Long

  def _seek(newPos: Long): Unit
}

object SeekableByteChannel {
  case class ChannelByteChannel(ch: channels.SeekableByteChannel)
    extends SeekableByteChannel
  with BufferByteChannel {

    override protected def _read(dst: ByteBuffer): Int = ch.read(dst)

    override def size: Long = ch.size
    override def _close(): Unit = ch.close()
    override def position(): Long = ch.position()

    override def _skip(n: Int): Unit = ch.position(ch.position() + n)
    override def _seek(newPos: Long): Unit = ch.position(newPos)
  }

  implicit def makeChannelByteChannel(ch: channels.SeekableByteChannel): ChannelByteChannel =
    ChannelByteChannel(ch)

  implicit def apply(path: paths.Path): ChannelByteChannel =
    ChannelByteChannel(
      newByteChannel(
        path
      )
    )
}

