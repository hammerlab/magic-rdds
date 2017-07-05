package org.hammerlab.hadoop

class SerializablePath
  extends Serializable {

  // Relative paths put their toString here, others put their uri.toString
  var str: String = _
  implicit var conf: Configuration = _

  def readResolve: Any = Path(str)
}
