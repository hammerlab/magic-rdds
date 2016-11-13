package org.hammerlab.magic.rdd.serde.util

import org.hammerlab.magic.test.spark.KryoSerializerSuite

/**
 * Test base-class that registers dummy case-class [[Foo]] for Kryo serde.
 */
class FooRegistrarTest
  extends KryoSerializerSuite(
    registrationRequired = false,
    referenceTracking = true
  ) {
  kryoRegister(classOf[Foo])
}
