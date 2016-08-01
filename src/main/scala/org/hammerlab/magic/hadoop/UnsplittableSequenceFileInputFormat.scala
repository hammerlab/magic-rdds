package org.hammerlab.magic.hadoop

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.SequenceFileInputFormat

/**
 * [[SequenceFileInputFormat]] guaranteed to be loaded in with the same splits it was written out with.
 */
class UnsplittableSequenceFileInputFormat[K, V] extends SequenceFileInputFormat[K, V] {
  override def isSplitable(fs: FileSystem, filename: Path): Boolean = false
}
