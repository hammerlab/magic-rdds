package org.hammerlab.magic.rdd.serde

import org.hammerlab.magic.test.rdd.VerifyRDDSerde
import org.hammerlab.magic.test.serde.util.{Foo, Foos}

trait SerdeRDDTest {
  self: VerifyRDDSerde =>

  def testSmallInts(ps: Int*): Unit = {
    test("rdd small ints") {
      verifyFileSizeListAndSerde(
        1 to 400,
        ps
      )
    }
  }

  def testMediumInts(ps: Int*): Unit = {
    test("rdd medium ints") {
      verifyFileSizeListAndSerde(
        (1 to 400).map(_ + 500),
        ps
      )
    }
  }

  def testLongs(ps: Int*): Unit = {
    test("rdd longs") {
      verifyFileSizeListAndSerde(
        (1 to 400).map(_ + 12345678L),
        ps
      )
    }
  }

  /**
   * Test serializing some [[Foo]]s.
   *
   * @param n  number of [[Foo]]s to make per partition
   * @param ps list of expected serialized partition sizes; if only 1 size is passed, 4 equally-sized partitions are
   *           assumed.
   */
  def testSomeFoos(n: Int, ps: Int*): Unit = {
    test(s"some foos $n") {
      verifyFileSizeListAndSerde(
        Foos(
          (
            // By convention, partition-size lists of length 1 or 2 are padded out to 4 partitions with their last
            // element.
            if (ps.size <= 2)
              4
            else
              ps.size
          ) * n,
          20
        ),
        ps
      )
    }
  }
}
