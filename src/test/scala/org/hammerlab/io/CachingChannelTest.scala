package org.hammerlab.io

import java.io.EOFException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import org.hammerlab.io.CachingChannel._
import org.hammerlab.io.SeekableByteChannel.ChannelByteChannel
import org.hammerlab.paths.Path
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

import scala.collection.mutable.ArrayBuffer

class MockChannel(path: Path)
  extends ChannelByteChannel(FileChannel.open(path)) {
  val reads = ArrayBuffer[(Long, Int)]()
  override def _read(dst: ByteBuffer): Int = {
    reads += position() → dst.remaining()
    super._read(dst)
  }
}

class CachingChannelTest
  extends Suite {
  test("cache") {
    val ch = new MockChannel(File("log4j.properties"))
    val buf = Buffer(40)
    val cc = ch.cache(blockSize = 64)

    ch.reads should be(Nil)

    def seekCheck(pos: Int,
                  expectedBytes: String,
                  expectedBlocks: Int,
                  expectedReads: (Int, Int)*): Unit = {
      cc.seek(pos)
      check(
        expectedBytes,
        expectedBlocks,
        expectedReads: _*
      )
    }

    def check(expectedBytes: String,
              expectedBlocks: Int,
              expectedReads: (Int, Int)*): Unit = {
      buf.clear()

      cc.readFully(buf)

      buf
        .array()
        .slice(0, 4)
        .map(_.toChar)
        .mkString("") should be(
          expectedBytes
        )

      // Still just one read from underlying channel
      ch.reads should be(expectedReads)
    }

        check(     "log4", 1, 0 → 64)
    seekCheck(  1, "og4j", 1, 0 → 64)
    seekCheck(  1, "og4j", 1, 0 → 64)
    seekCheck(  0, "log4", 1, 0 → 64)
    seekCheck(  7, "ootC", 1, 0 → 64)
    seekCheck( 48, "cons", 2, 0 → 64, 64 → 64)
    seekCheck( 48, "cons", 2, 0 → 64, 64 → 64)
    seekCheck( 47, ".con", 2, 0 → 64, 64 → 64)
    seekCheck(  2, "g4j.", 2, 0 → 64, 64 → 64)
    seekCheck(217, "out.", 4, 0 → 64, 64 → 64, 192 → 64, 256 → 20)

    intercept[EOFException] {
      cc.seek(237)
      buf.clear
      cc.readFully(buf)
    }

    // Test a partial read
    cc.seek(237)
    buf.clear
    cc.read(buf) should be(39)
    buf
      .array()
      .slice(0, 4)
      .map(_.toChar)
      .mkString("") should be(
        "n=%d"
      )

    ch.reads should be(Seq(0 → 64, 64 → 64, 192 → 64, 256 → 20))
  }
}
