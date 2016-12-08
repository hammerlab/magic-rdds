package org.hammerlab.magic.rdd.serde

import org.hammerlab.magic.rdd.serde.util.FooRegistrarTest
import org.hammerlab.spark.test.suite.{ JavaSerializerSuite, KryoSerializerSuite }
import org.hammerlab.test.version.Util

// Java serde, Foos not registered, not compressed.
class JavaSequenceFileRDDTest
  extends SequenceFileRDDTest
    with SerdeRDDTest
    with JavaSerializerSuite {

  testSmallInts(9475)
  testMediumInts(9475)
  testLongs(9575)

  testSomeFoos(   1,    222)
  testSomeFoos(  10,   1365)
  testSomeFoos( 100,  12915)
  testSomeFoos(1000, 128335)
}

// Java serde, Foos not registered, compressed.
class JavaBZippedSequenceFileRDDTest
  extends BZippedSequenceFileRDDTest
    with SerdeRDDTest
    with JavaSerializerSuite {

  testSmallInts(648, 655, 666, 651)
  testMediumInts(652, 655, 668, 646)
  testLongs(662, 676, 664, 675)

  if (Util.is2_10) {
    testSomeFoos(   1,  400,  394,  396,  395)
    testSomeFoos(  10,  526,  533,  534,  528)
    testSomeFoos( 100,  826,  856,  888,  860)
    testSomeFoos(1000, 2113, 2110, 2112, 2151)
  } else {
    testSomeFoos(   1,  403,  403,  405,  404)
    testSomeFoos(  10,  530,  534,  525,  530)
    testSomeFoos( 100,  860,  845,  884,  835)
    testSomeFoos(1000, 2117, 2148, 2083, 2136)
  }
}

// Kryo serde, Foos not registered, not compressed.
class KryoSequenceFileRDDTest
  extends KryoSerializerSuite(registrationRequired = false)
    with SequenceFileRDDTest
    with SerdeRDDTest {

  testSmallInts(1532, 1595, 1595, 1595)
  testMediumInts(1595)
  testLongs(1795)

  testSomeFoos(   1,   168)
  testSomeFoos(  10,   825)
  testSomeFoos( 100,  7492,  7555)
  testSomeFoos(1000, 74732, 74795)
}

// Kryo serde, register Foo classes, no compression.
class KryoSequenceFileFooRDDTest
  extends FooRegistrarTest
    with SequenceFileRDDTest
    with SerdeRDDTest {

  testSmallInts(1532, 1595)
  testMediumInts(1595)
  testLongs(1795)

  testSomeFoos(   1,   131)
  testSomeFoos(  10,   455)
  testSomeFoos( 100,  3752,  3815,  3815,  3815)
  testSomeFoos(1000, 37392, 37455, 37455, 37455)
}

// Kryo serde, register Foo classes, compress.
class KryoBzippedSequenceFileFooRDDTest
  extends FooRegistrarTest
    with BZippedSequenceFileRDDTest
    with SerdeRDDTest {

  testSmallInts(463, 456, 447, 445)
  testMediumInts(443, 446, 447, 444)
  testLongs(461, 462, 460, 461)

  testSomeFoos(   1,  303)
  testSomeFoos(  10,  355,  361,  360,  361)
  testSomeFoos( 100,  701,  731,  708,  724)
  testSomeFoos(1000, 1976, 1722, 1700, 1665)
}
