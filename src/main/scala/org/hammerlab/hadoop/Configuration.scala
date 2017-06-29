package org.hammerlab.hadoop

import java.io.{ ObjectInputStream, ObjectOutputStream }

import org.apache.hadoop.conf.{ Configuration ⇒ HadoopConfiguration }
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

class Configuration(@transient var value: HadoopConfiguration)
  extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    value = new HadoopConfiguration(false)
    value.readFields(in)
  }
}

object Configuration {

  def apply(loadDefaults: Boolean = true): Configuration =
    new HadoopConfiguration(loadDefaults)

  def apply(conf: HadoopConfiguration): Configuration =
    new Configuration(conf)

  implicit def wrapConfiguration(conf: HadoopConfiguration): Configuration =
    apply(conf)

  implicit def unwrapConfiguration(conf: Configuration): HadoopConfiguration =
    conf.value

  implicit def unwrapSerializableConfigurationBroadcast(confBroadcast: Broadcast[Configuration]): Configuration =
    confBroadcast
      .value
      .value

  implicit def sparkContextToHadoopConfiguration(sc: SparkContext): Configuration =
    sc.hadoopConfiguration

  implicit class ConfWrapper(val conf: HadoopConfiguration) extends AnyVal {
    def serializable: Configuration =
      Configuration(conf)
  }
}
