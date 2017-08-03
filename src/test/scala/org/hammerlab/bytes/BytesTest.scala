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

  test("format") {

    def check(n: Long, expected: String): Unit = {
      Bytes.format(n) should be(expected)
    }

    check(   0,    "0")
    check(   1,    "1")
    check(   2,    "2")

    check(   9,    "9")
    check(  10,   "10")
    check(  11,   "11")

    check(  99,   "99")
    check( 100,  "100")
    check( 101,  "101")

    check( 999,  "999")
    check(1000, "1000")
    check(1001, "1001")

    check(1023, "1023")
    check(1024, "1K")
    check(1025, "1.0K")

    check(1075, "1.0K")
    check(1076, "1.1K")
    check(1077, "1.1K")

    check(1177, "1.1K")
    check(1178, "1.2K")
    check(1179, "1.2K")

    check(1996, "1.9K")
    check(1997, "2.0K")
    check(1998, "2.0K")

    check(2047, "2.0K")
    check(2048, "2K")
    check(2049, "2.0K")

    check(10188, "9.9K")
    check(10189, "10.0K")
    check(10190, "10.0K")

    check(10239, "10.0K")
    check(10240, "10K")
    check(10241, "10.0K")

    check(10291, "10.0K")
    check(10292, "10.1K")
    check(10293, "10.1K")

    check(10342, "10.1K")
    check(10343, "10.1K")
    check(10344, "10.1K")

    check(10393, "10.1K")
    check(10394, "10.2K")
    check(10395, "10.2K")

    check((  11 << 10) -  52, "10.9K")
    check((  11 << 10) -  51, "11.0K")
    check((  11 << 10) -  50, "11.0K")

    check((  11 << 10) -   1, "11.0K")
    check((  11 << 10) -   0,   "11K")
    check((  11 << 10) +   1, "11.0K")

    check(( 100 << 10) -  52, "99.9K")
    check(( 100 << 10) -  51,  "100K")
    check(( 100 << 10) -  50,  "100K")

    check(( 100 << 10) + 511,  "100K")
    check(( 100 << 10) + 512,  "101K")
    check(( 100 << 10) + 513,  "101K")

    check(( 101 << 10) + 511,  "101K")
    check(( 101 << 10) + 512,  "102K")
    check(( 101 << 10) + 513,  "102K")

    check(( 999 << 10) -   1,  "999K")
    check(( 999 << 10) +   0,  "999K")
    check(( 999 << 10) +   1,  "999K")

    check(( 999 << 10) + 511,  "999K")
    check(( 999 << 10) + 512, "1000K")
    check(( 999 << 10) + 513, "1000K")

    check((1000 << 10) + 511, "1000K")
    check((1000 << 10) + 512, "1001K")
    check((1000 << 10) + 513, "1001K")

    check((1023 << 10) + 511, "1023K")
    check((1023 << 10) + 512, "1024K")
    check((1023 << 10) + 513, "1024K")

    check((   1 << 20) -   1, "1024K")
    check((   1 << 20) +   0,    "1M")
    check((   1 << 20) +   1,  "1.0M")

    check((   1 << 30) -   1, "1024M")
    check((   1 << 30) +   0,    "1G")
    check((   1 << 30) +   1,  "1.0G")

    check((  1L << 40) -   1, "1024G")
    check((  1L << 40) +   0,    "1T")
    check((  1L << 40) +   1,  "1.0T")

    check((  1L << 50) -   1, "1024T")
    check((  1L << 50) +   0,    "1P")
    check((  1L << 50) +   1,  "1.0P")

    check((  1L << 60) -   1, "1024P")
    check((  1L << 60) +   0,    "1E")
    check((  1L << 60) +   1,  "1.0E")
  }
}
