package org.hammerlab.hadoop.splits

import hammerlab.path._
import org.apache.hadoop.mapreduce.lib.input
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat.{ SPLIT_MAXSIZE, setInputPaths }
import org.apache.hadoop.mapreduce.{ InputSplit, Job, TaskAttemptContext }
import org.apache.hadoop.{ fs, mapreduce }
import org.hammerlab.hadoop.Configuration

import scala.collection.JavaConverters._

object FileSplits {

  def asJava(path: Path, maxSplitSize: MaxSplitSize)(
      implicit conf: Configuration
  ): java.util.List[InputSplit] = {

    val job = Job.getInstance(conf, s"$path:file-splits")

    val jobConf = job.getConfiguration

    jobConf.setLong(SPLIT_MAXSIZE, maxSplitSize)

    setInputPaths(job, new fs.Path(path.uri))

    val fif =
      new input.FileInputFormat[Any, Any] {
        // Hadoop API requires us to have a stub here, though it is not used
        override def createRecordReader(split: InputSplit,
                                        context: TaskAttemptContext): mapreduce.RecordReader[Any, Any] =
          ???
      }

    fif.getSplits(job)
  }

  def apply(path: Path,
            maxSplitSize: MaxSplitSize)(
      implicit conf: Configuration
  ): Seq[FileSplit] = {
    asJava(path, maxSplitSize)
      .asScala
      .map(_.asInstanceOf[input.FileSplit]: FileSplit)
  }
}
