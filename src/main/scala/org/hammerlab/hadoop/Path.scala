package org.hammerlab.hadoop

import java.net.URI

import org.apache.hadoop.fs

case class Path(uri: URI) {
  override def toString: String = uri.toString
}

object Path {
  implicit def fromHadoopPath(path: fs.Path): Path = Path(path.toUri)
  implicit def toHadoopPath(path: Path): fs.Path = new fs.Path(path.uri)
}
