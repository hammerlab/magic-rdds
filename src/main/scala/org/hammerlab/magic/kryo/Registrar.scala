package org.hammerlab.magic.kryo

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.magic.rdd.{KeySamples, RDDStats, RunLengthRDD}

class Registrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    KeySamples.registerKryo(kryo)
    RunLengthRDD.registerKryo(kryo)
    RDDStats.registerKryo(kryo)
  }
}
