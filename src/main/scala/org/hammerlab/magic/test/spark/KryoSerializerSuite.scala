package org.hammerlab.magic.test.spark

class KryoSerializerSuite(registrar: String = "org.hammerlab.magic.kryo.Registrar",
                          registrationRequired: Boolean = true,
                          referenceTracking: Boolean = false)
  extends SparkSuite {

  conf
    .set("spark.kryo.referenceTracking", referenceTracking.toString)
    .set("spark.kryo.registrationRequired", registrationRequired.toString)
    .set("spark.kryo.registrator", registrar)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
}
