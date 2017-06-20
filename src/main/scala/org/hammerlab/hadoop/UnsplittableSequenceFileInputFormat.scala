package org.hammerlab.hadoop

import java.io.IOException

import org.apache.hadoop.fs.{ FileStatus, FileSystem, Path ⇒ HPath }
import org.apache.hadoop.mapred.{ JobConf, SequenceFileInputFormat }

/**
 * [[SequenceFileInputFormat]] guaranteed to be loaded in with the same splits it was written out with.
 */
class UnsplittableSequenceFileInputFormat[K, V] extends SequenceFileInputFormat[K, V] {
  override def isSplitable(fs: FileSystem, filename: HPath): Boolean = false

  /**
   * Ensure that partitions are read back in in the same order they were written.
   */
  override def listStatus(job: JobConf): Array[FileStatus] = {
    super.listStatus(job).sortBy(file ⇒ {
      val basename = file.getPath.getName
      if (!basename.startsWith("part-")) {
        throw new IOException(s"Bad partition file: $basename")
      }
      basename.drop("part-".length).toInt
    })
  }
}
