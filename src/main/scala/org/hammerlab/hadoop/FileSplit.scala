package org.hammerlab.hadoop

import org.apache.hadoop.mapreduce.lib.input

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
  def apply(split: input.FileSplit): FileSplit =
    FileSplit(
      split.getPath,
      split.getStart,
      split.getLength,
      split.getLocations
    )

  implicit def conv(split: input.FileSplit): FileSplit =
    apply(split)
}
