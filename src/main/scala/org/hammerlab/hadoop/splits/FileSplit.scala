package org.hammerlab.hadoop.splits

import org.apache.hadoop.mapreduce.lib.input
import org.hammerlab.hadoop.{ Configuration, Path }

/**
 * Case-class sugar over Hadoop [[input.FileSplit]]
 */
case class FileSplit(path: Path,
                     start: Long,
                     length: Long,
                     locations: Array[String])
  extends input.FileSplit {
  def end = start + length
}

object FileSplit {
  def apply(split: input.FileSplit)(implicit conf: Configuration): FileSplit =
    FileSplit(
      split.getPath,
      split.getStart,
      split.getLength,
      split.getLocations
    )

  implicit def conv(split: input.FileSplit)(implicit conf: Configuration): FileSplit =
    apply(split)
}
