package org.hammerlab.hadoop.splits

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input

/**
 * Case-class sugar over Hadoop [[input.FileSplit]]
 */
case class FileSplit(path: Path,
                     start: Long,
                     length: Long,
                     locations: Array[String]) {
  def end = start + length
}

object FileSplit {
  implicit def apply(split: input.FileSplit): FileSplit =
    FileSplit(
      split.getPath,
      split.getStart,
      split.getLength,
      split.getLocations
    )

  implicit def apply(split: FileSplit): input.FileSplit =
    new input.FileSplit(
      split.path,
      split.start,
      split.length,
      split.locations
    )
}
