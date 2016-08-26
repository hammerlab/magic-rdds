package org.hammerlab.magic.rdd.sliding

import com.esotericsoftware.kryo.Kryo

object SlidingRDDKryoRegistrar {
  def register(kryo: Kryo): Unit = {
    // Used in BorrowElemsRDD's partitionOverridesBroadcast.
    kryo.register(classOf[Map[Int, Int]])
    kryo.register(Class.forName("scala.collection.immutable.Map$EmptyMap$"))
  }
}
