package org.hammerlab.io

import java.io.IOException
import java.nio.ByteBuffer
import java.util

import grizzled.slf4j.Logging
import org.hammerlab.bytes._
import org.hammerlab.io.CachingChannel.Config
import org.hammerlab.math.ceil

import scala.math.{ max, min }

/**
 * [[SeekableByteChannel]] that wraps another [[SeekableByteChannel]] and caches data read from the latter in chunks of
 * size [[config.blockSize]] (quantized to boundaries at whole multiples of [[config.blockSize]])
 *
 * @param channel underlying channel to provide a caching layer over
 */
case class CachingChannel[Channel <: SeekableByteChannel](channel: Channel)(
    implicit config: Config
)
  extends SeekableByteChannel
    with BufferByteChannel
    with Logging {

  val Config(blockSize, maxReadAttempts, maximumSize) = config
  val maxNumBlocks = ceil(maximumSize, blockSize).toInt

  private val _buffer = ByteBuffer.allocate(blockSize)

  val blocks =
    new util.LinkedHashMap[Long, ByteBuffer](
      (maximumSize / blockSize).toInt,
      0.7f,
      true
    ) {
      override def removeEldestEntry(eldest: util.Map.Entry[Long, ByteBuffer]): Boolean = {
        if (size() > maxNumBlocks) {
          debug(s"Size ${size()} > max num blocks $maxNumBlocks (total size $maximumSize)")
          true
        } else
          false
      }
    }

  def getBlock(idx: Long): ByteBuffer =
    if (!blocks.containsKey(idx)) {
      _buffer.clear()
      val start = idx * blockSize
      channel.seek(start)
      if (channel.size - start < _buffer.limit) {
        _buffer.limit((channel.size - start).toInt)
      }
      val bytesToRead = _buffer.remaining()
      var attempts = 0
      val end = start + bytesToRead
      debug(s"Fetching block $idx: [$start,$end)")
      while (channel.position() < end && attempts < maxReadAttempts) {
        channel.read(_buffer)
        attempts += 1
      }

      if (channel.position() < end) {
        throw new IOException(
          s"Read ${channel.position() - start} of $bytesToRead bytes from $start in $attempts attempts"
        )
      }

      val dupe = ByteBuffer.allocate(bytesToRead)
      _buffer.position(0)
      dupe.put(_buffer)
      blocks.put(idx, dupe)
      debug(s"Fetched block $idx: [$start,$end)")
      dupe
    } else
      blocks.get(idx)

  override def size: Long = channel.size

  override def _seek(newPos: Long): Unit = channel.seek(newPos)

  override def _read(dst: ByteBuffer): Int = {
    val start = position()
    if (start == size)
      return -1

    val end = min(size, start + dst.remaining())

    val startBlock = start / blockSize
    val endBlock = end / blockSize

    var bytesIdx = dst.position()
    var numRead = 0

    for {
      idx ← startBlock to endBlock
      blockStart = idx * blockSize
      blockEnd = (idx + 1) * blockSize
      from = max((start - blockStart).toInt, 0)
      to = (min(end, blockEnd) - blockStart).toInt
      blockBuffer = getBlock(idx)
    } {
      blockBuffer.limit(to)
      blockBuffer.position(from)
      dst.put(blockBuffer)
      numRead += to - from
    }

    numRead
  }

  override def _close(): Unit =
    channel.close()
}

object CachingChannel {

  /**
   * Configuration options for a [[CachingChannel]]
   *
   * @param blockSize size of blocks to cache
   * @param maxReadAttempts all read/skip operations require the full requested number of bytes to be returned in at most
   *                        this many attempts, or they will throw an [[IOException]].
   * @param maximumSize evict blocks from cache to avoid growing beyond this size
   */
  case class Config(blockSize: Int = 64.KB.toInt,
                    maxReadAttempts: Int = 2,
                    maximumSize: Long = 64.MB)

  object Config {
    implicit val default = Config()
  }

  implicit def makeCachingChannel[Channel <: SeekableByteChannel](channel: Channel)(
      implicit config: Config
  ): CachingChannel[Channel] =
    CachingChannel(channel)

  implicit class AddCaching[Channel <: SeekableByteChannel](channel: Channel) {
    def cache(implicit config: Config): CachingChannel[Channel] =
      makeCachingChannel(channel)

    def cache(blockSize: Int = 64.KB.toInt,
              maxReadAttempts: Int = 2,
              maximumSize: Long = 64.MB): CachingChannel[Channel] =
      makeCachingChannel(
        channel
      )(
        Config(
          blockSize,
          maxReadAttempts,
          maximumSize
        )
      )
  }
}
