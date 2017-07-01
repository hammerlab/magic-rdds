package org.hammerlab.hadoop

import java.net.URI

import org.apache.hadoop.fs
import org.apache.hadoop.fs.{ FSDataInputStream, FSDataOutputStream, FileSystem }

/**
 * Attempt at a serializable and otherwise-embellished wrapper for [[fs.Path]].
 *
 * Pretty clumsy in its current state due to the requirement of a [[Configuration]] in order to get at path-metadata via
 * a [[FileSystem]].
 *
 * In particular, serialization carries the whole [[Configuration]] along with it which leads to silly amounts of data
 * being transmitted.
 */
case class Path(path: fs.Path)(implicit conf: Configuration) {

  def uri: URI = path.toUri

  def fullyQualifiedURI: URI =
    filesystem.makeQualified(path).toUri

  override def toString: String = uri.toString

  def inputStream: FSDataInputStream =
    filesystem.open(this)

  def outputStream: FSDataOutputStream =
    filesystem.create(this)

  private lazy val filesystem: FileSystem =
    path.getFileSystem(conf)

  def length: Long =
    filesystem.getFileStatus(this).getLen

  def exists: Boolean =
    filesystem.exists(this)
}

object Path {

  // Turn off checksums for the local filesystem, by default
  disableLocalFSChecksums

  def apply(uri: URI)(implicit conf: Configuration): Path = Path(new fs.Path(uri))
  def apply(uriStr: String)(implicit conf: Configuration): Path = Path(new fs.Path(uriStr))

  implicit def fromHadoopPath(path: fs.Path)(implicit conf: Configuration): Path = Path(path)
  implicit def toHadoopPath(path: Path): fs.Path = path.path
  implicit def readPath(path: Path)(implicit conf: Configuration): FSDataInputStream = path.inputStream
  implicit def writePath(path: Path)(implicit conf: Configuration): FSDataOutputStream = path.outputStream
}
