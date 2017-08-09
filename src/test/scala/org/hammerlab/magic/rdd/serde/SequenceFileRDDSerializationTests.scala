package org.hammerlab.magic.rdd.serde

import org.hammerlab.magic.rdd.serde.util.FooRegistrarTest
import org.hammerlab.spark.test.suite.{ JavaSerializerSuite, KryoSparkSuite }
import org.hammerlab.test.version.Util.is2_10

// Java serde, Foos not registered, not compressed.
class JavaSequenceFileRDDTest
  extends SequenceFileRDDTest
    with SerdeRDDTest
    with JavaSerializerSuite {

  testSmallInts(14215)
  testMediumInts(14215)
  testLongs(14215)

  testSomeFoos(   1,    279)
  testSomeFoos(  10,   1935)
  testSomeFoos( 100,  18675)
  testSomeFoos(1000, 185895)
}

// Java serde, Foos not registered, compressed.
class JavaBZippedSequenceFileRDDTest
  extends BZippedSequenceFileRDDTest
    with SerdeRDDTest
    with JavaSerializerSuite {

  testSmallInts(650, 659, 672, 658)
  testMediumInts(662, 663, 678, 656)
  testLongs(665, 681, 664, 672)

  if (is2_10) {
    testSomeFoos(   1,  401,  406,  402,  407)
    testSomeFoos(  10,  533,  538,  540,  534)
    testSomeFoos( 100,  870,  874,  885,  850)
    testSomeFoos(1000, 2138, 2137, 2160, 2159)
  } else {
    testSomeFoos(   1,  409,  411,  410,  409)
    testSomeFoos(  10,  535,  537,  535,  535)
    testSomeFoos( 100,  868,  856,  886,  880)
    testSomeFoos(1000, 2147, 2173, 2182, 2189)
  }
}

// Kryo serde, Foos not registered, not compressed.
class KryoSequenceFileRDDTest
  extends KryoSparkSuite(registrationRequired = false)
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

  testSmallInts( 463, 456, 447, 445)
  testMediumInts(443, 446, 447, 444)
  testLongs(     461, 462, 460, 461)

  testSomeFoos(   1,  303)
  testSomeFoos(  10,  355,  358,  360,  361)
  testSomeFoos( 100,  701,  731,  708,  724)
  testSomeFoos(1000, 1974, 1722, 1686, 1665)
}
