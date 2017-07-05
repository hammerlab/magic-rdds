package org.hammerlab.io

import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import org.hammerlab.io.CachingChannel._
import org.hammerlab.io.SeekableByteChannel.ChannelByteChannel
import org.hammerlab.paths.Path
import org.hammerlab.test.Suite
import org.hammerlab.bytes._

import scala.collection.mutable.ArrayBuffer

class MockChannel(path: Path)
  extends ChannelByteChannel(FileChannel.open(path)) {
  val reads = ArrayBuffer[(Long, Int)]()
  override protected def _read(dst: ByteBuffer): Unit = {
    reads += position() → dst.remaining()
    super._read(dst)
  }
}

class CachingChannelTest
  extends Suite {
  test("cache") {
    val ch = new MockChannel(Path("/Users/ryan/c/hl/spark-bam/src/test/resources/1.2203053-2211029.bam"))
    val buf = Buffer(128)
    val cc = ch.cache

    ch.reads should be(Nil)

    cc.read(buf)

    // One read from underlying channel
    ch.reads should be(Seq(0 → 65536))

    cc.blocks.size() should be(1)
    buf.array().slice(0, 4) should be(Array(31, -117, 8, 4))

    buf.clear()

    cc.seek(1)
    cc.read(buf)

    buf.array().slice(0, 4) should be(Array(-117, 8, 4, 0))

    // Still just one read from underlying channel
    ch.reads should be(Seq(0 → 65536))
  }
}
