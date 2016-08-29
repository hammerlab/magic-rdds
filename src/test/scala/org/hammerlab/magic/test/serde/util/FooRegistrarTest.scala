package org.hammerlab.magic.test.serde.util

import org.hammerlab.magic.test.spark.{KryoSerializerSuite, SparkSuite}

/**
 * Test base-class that registers dummy case-class [[Foo]] for Kryo serde.
 */
class FooRegistrarTest
  extends KryoSerializerSuite(
    registrar = "org.hammerlab.magic.test.serde.util.FooKryoRegistrator",
    registrationRequired = false,
    referenceTracking = true
  )
