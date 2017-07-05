package org.hammerlab.hadoop.splits

import org.apache.hadoop.mapreduce
import org.apache.hadoop.mapreduce.lib.input
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat.{ SPLIT_MAXSIZE, setInputPaths }
import org.apache.hadoop.mapreduce.{ InputSplit, Job, TaskAttemptContext }
import org.hammerlab.hadoop.{ Configuration, Path }

import scala.collection.JavaConverters._

object FileSplits {

  def apply(path: Path)(
      implicit
      conf: Configuration
  ): Seq[FileSplit] =
    apply(path, MaxSplitSize())

  def apply(path: Path, maxSplitSize: MaxSplitSize)(
      implicit conf: Configuration
  ): Seq[FileSplit] = {

    val job = Job.getInstance(conf, s"$path:file-splits")

    val jobConf = job.getConfiguration

    jobConf.setLong(SPLIT_MAXSIZE, maxSplitSize)

    setInputPaths(job, path)

    val fif =
      new input.FileInputFormat[Any, Any] {
        // Hadoop API requires us to have a stub here, though it is not used
        override def createRecordReader(split: InputSplit,
                                        context: TaskAttemptContext): mapreduce.RecordReader[Any, Any] =
          ???
      }

    fif
      .getSplits(job)
      .asScala
      .map(_.asInstanceOf[input.FileSplit]: FileSplit)
  }
}
