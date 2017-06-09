package org.hammerlab.hadoop

import java.io.{ ObjectInputStream, ObjectOutputStream }

import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast

class SerializableConfiguration(@transient var value: Configuration)
  extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    value = new Configuration(false)
    value.readFields(in)
  }
}

object SerializableConfiguration {
  implicit def unwrapSerializableConfiguration(conf: SerializableConfiguration): Configuration = conf.value
  implicit def unwrapSerializableConfigurationBroadcast(confBroadcast: Broadcast[SerializableConfiguration]): Configuration = confBroadcast.value.value

  def apply(conf: Configuration): SerializableConfiguration =
    new SerializableConfiguration(conf)

  implicit class ConfWrapper(val conf: Configuration) extends AnyVal {
    def serializable: SerializableConfiguration =
      SerializableConfiguration(conf)
  }
}
