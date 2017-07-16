package org.hammerlab.io

import java.io.{ EOFException, IOException, InputStream }
import java.nio.channels.FileChannel

import Array.fill
import org.hammerlab.hadoop.Configuration
import org.hammerlab.io.CachingChannel._
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

case class InputStreamStub(reads: String*)
  extends InputStream {
  val bytes =
    reads
      .flatMap(
        _
          .map(_.toInt)
          .toVector :+ -1
      )
      .iterator

  override def read(): Int =
    if (bytes.hasNext)
      bytes.next()
    else
      -1
}

class ByteChannelTest
  extends Suite {
  test("incomplete InputStream read") {
    val ch: ByteChannel =
      InputStreamStub(
        "12345",
        "67890",
        "1",
        "",
        "234"
      )

    val b4 = Buffer(4)

    ch.readFully(b4)
    b4.array.map(_.toChar).mkString("") should be("1234")
    ch.position() should be(4)

    b4.position(0)
    ch.readFully(b4)
    b4.array.map(_.toChar).mkString("") should be("5678")
    ch.position() should be(8)

    b4.position(0)
    intercept[IOException] {
      ch.readFully(b4)
    }.getMessage should be("Got 3 (2 then 1) of 4 bytes in 2 attempts from position 8")
  }

  test("read single bytes") {
    val ch: ByteChannel =
      InputStreamStub(
        "12345",
        "67890",
        "1",
        "",
        "234"
      )

    ch.position() should be(0)
    ch.read().toChar should be('1')
    ch.position() should be(1)
    ch.read().toChar should be('2')
    ch.position() should be(2)
    ch.read().toChar should be('3')
    ch.position() should be(3)
  }

  def check(ch: ByteChannel): Unit = {
    ch.position should be(0)

    ch.readString(4, includesNull = false) should be("log4")
    ch.position should be(4)

    ch.read.toChar should be('j')
    ch.position should be(5)

    val bytes = fill(6)(0.toByte)
    ch.read(bytes) should be(6)
    bytes.map(_.toChar).mkString("") should be(".rootC")
    ch.position should be(11)

    ch.read.toChar should be('a')
    ch.position should be(12)

    val buf = Buffer(7)
    ch.read(buf) should be(7)
    buf.array().map(_.toChar).mkString("") should be("tegory=")
    ch.position should be(19)

    ch.skip(250)
    ch.position should be(269)

    ch.read(bytes, 1, 2)
    bytes.slice(1, 3).map(_.toChar).mkString("") should be(": ")
    ch.position should be(271)

    buf.clear()
    ch.read(buf) should be(5)
    buf.array().slice(0, 5).map(_.toChar).mkString("") should be("%m%n\n")
    ch.position should be(276)

    ch.read should be(-1)
    ch.read(bytes) should be(-1)
    buf.clear
    buf.limit(0)
    ch.read(buf) should be(0)
    buf.clear()
    ch.read(buf) should be(-1)
  }

  test("InputStreamByteChannel") {
    check(File("log4j.properties").inputStream)
  }

  test("IteratorByteChannel") {
    check(
      File("log4j.properties")
        .readBytes
        .iterator
    )
  }

  test("ChannelByteChannel") {
    check(FileChannel.open(File("log4j.properties").path): SeekableByteChannel)
  }

  test("channel array read") {
    val ch: SeekableByteChannel = FileChannel.open(File("log4j.properties").path)
    val bytes = fill(4)(0.toByte)
    ch.read(bytes)
    ch.position() should be(4)
  }

  test("CachingChannel") {
    implicit val conf = Configuration()
    val path = File("log4j.properties").path

    val ch = SeekableByteChannel(path)

    val cc = SeekableByteChannel(path).cache

    val expectedBytes = path.readBytes
    for {
      (i, expectedByte) ← 0 until 100 zip expectedBytes
    } {
      ch.seek(i)
      cc.seek(i)

      withClue(s"idx: $i: ") {
        cc.getInt should be(ch.getInt)
      }

      ch.seek(i)
      cc.seek(i)

      withClue(s"idx: $i: ") {
        cc.read() should be(expectedByte)
        ch.read() should be(expectedByte)
      }
    }
  }
}
