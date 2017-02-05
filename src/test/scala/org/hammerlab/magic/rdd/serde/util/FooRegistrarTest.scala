package org.hammerlab.magic.rdd.serde.util

import org.hammerlab.spark.test.suite.KryoSparkSuite

/**
 * Test base-class that registers dummy case-class [[Foo]] for Kryo serde.
 */
class FooRegistrarTest
  extends KryoSparkSuite(
    registrationRequired = false,
    referenceTracking = true
  ) {
  register(classOf[Foo])
}
