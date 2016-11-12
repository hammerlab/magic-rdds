package org.hammerlab.magic.rdd.serde

import org.hammerlab.magic.test.rdd.SequenceFileRDDTest
import org.hammerlab.magic.test.serde.SequenceFileRDDTest
import org.hammerlab.magic.test.version.Util
import org.hammerlab.magic.test.serde.util.FooRegistrarTest
import org.hammerlab.magic.test.spark.{JavaSerializerSuite, KryoSerializerSuite}

// Java serde, Foos not registered, not compressed.
class JavaSequenceFileRDDTest
  extends SequenceFileRDDTest
    with SerdeRDDTest
    with JavaSerializerSuite {

  testSmallInts(9475)
  testMediumInts(9475)
  testLongs(9575)

  testSomeFoos(   1,    223)
  testSomeFoos(  10,   1375)
  testSomeFoos( 100,  13015)
  testSomeFoos(1000, 129335)
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
    testSomeFoos(   1,  404,  407,  409,  398)
    testSomeFoos(  10,  531,  539,  528,  532)
    testSomeFoos( 100,  864,  854,  875,  835)
    testSomeFoos(1000, 2122, 2113, 2133, 2147)
  } else {
    testSomeFoos(   1,  406,  403,  403,  397)
    testSomeFoos(  10,  536,  538,  528,  529)
    testSomeFoos( 100,  874,  856,  867,  847)
    testSomeFoos(1000, 2089, 2134, 2076, 2150)
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

  testSomeFoos(   1,   169)
  testSomeFoos(  10,   835)
  testSomeFoos( 100,  7592,  7655)
  testSomeFoos(1000, 75772, 75835)
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

  testSomeFoos(   1,  301)
  testSomeFoos(  10,  353,  361,  360,  359)
  testSomeFoos( 100,  716,  729,  706,  722)
  testSomeFoos(1000, 1985, 1714, 1674, 1665)
}
