package org.hammerlab.hadoop

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

object Registrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[SerializableConfiguration])
  }
}
