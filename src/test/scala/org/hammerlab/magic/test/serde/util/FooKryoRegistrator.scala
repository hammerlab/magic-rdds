package org.hammerlab.magic.test.serde.util

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class FooKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Foo])
  }
}
