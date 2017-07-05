package org.hammerlab.bytes

import org.hammerlab.test.Suite

class BytesTest
  extends Suite {

  def check(inputStr: String,
            expectedStr: String,
            expectedBytes: Long): Unit = {
    val size = Bytes(inputStr)
    size.toString should be(expectedStr)
    size.bytes should be(expectedBytes)
  }

  test("parsing") {

    check("0", "0B", 0)
    check("1b", "1B", 1)
    check("123B", "123B", 123)

    check("1kb", "1KB", 1L << 10)
    check("10k", "10KB", 10L << 10)
    check("123KB", "123KB", 123L << 10)

    check("1mb", "1MB", 1L << 20)
    check("10m", "10MB", 10L << 20)
    check("123MB", "123MB", 123L << 20)

    check("1gb", "1GB", 1L << 30)
    check("10g", "10GB", 10L << 30)
    check("123GB", "123GB", 123L << 30)

    check("1tb", "1TB", 1L << 40)
    check("10t", "10TB", 10L << 40)
    check("123TB", "123TB", 123L << 40)

    check("1pb", "1PB", 1L << 50)
    check("10p", "10PB", 10L << 50)
    check("123PB", "123PB", 123L << 50)

    check("1eb", "1EB", 1L << 60)
    check("2e", "2EB", 2L << 60)

    // 2^63 bytes: Long.MAX_VALUE + 1
    intercept[BytesOverflowException] {Bytes("8eb") }
    intercept[BytesOverflowException] {Bytes("8192pb") }
    intercept[BytesOverflowException] {Bytes("8388608tb") }
    intercept[NumberFormatException] {Bytes("8589934592gb") }

    intercept[BadBytesString] {Bytes("") }
    intercept[BadBytesString] {Bytes("1fb") }
    intercept[BadBytesString] {Bytes("gb") }
    intercept[BadBytesString] {Bytes("gb") }
  }

  test("wrappers") {
    32.B  should be( B(32))
    32.KB should be(KB(32))
    32.MB should be(MB(32))
    32.GB should be(GB(32))
    32.TB should be(TB(32))
    32.PB should be(PB(32))
    32.EB should be(EB(32))
  }
}
