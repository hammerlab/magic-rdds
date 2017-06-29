package org.hammerlab.hadoop

import java.net.URI

import org.apache.hadoop.fs
import org.apache.hadoop.fs.{ FSDataInputStream, FSDataOutputStream, FileSystem }

case class Path(uri: URI) {
  override def toString: String = uri.toString

  def inputStream(implicit conf: Configuration): FSDataInputStream =
    filesystem.open(this)

  def outputStream(implicit conf: Configuration): FSDataOutputStream =
    filesystem.create(this)

  def filesystem(implicit conf: Configuration): FileSystem =
    FileSystem.get(uri, conf)

  def length(implicit conf: Configuration): Long =
    filesystem.getFileStatus(this).getLen

  def exists(implicit conf: Configuration): Boolean =
    filesystem.exists(this)
}

object Path {

  // Turn off checksums for the local filesystem, by default
  disableLocalFSChecksums

  def apply(uriStr: String): Path = Path(new URI(uriStr))

  implicit def fromHadoopPath(path: fs.Path): Path = Path(path.toUri)
  implicit def toHadoopPath(path: Path): fs.Path = new fs.Path(path.uri)
  implicit def readPath(path: Path)(implicit conf: Configuration): FSDataInputStream = path.inputStream
  implicit def writePath(path: Path)(implicit conf: Configuration): FSDataOutputStream = path.outputStream
}
