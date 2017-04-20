package org.hammerlab.magic.util

import java.io.OutputStream

import grizzled.slf4j.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.SparkContext

/**
 * Convenience APIs for classes that can persist themselves to disk.
 */
trait Saveable
  extends Logging {
  def save(os: OutputStream): Unit

  def save(sc: SparkContext, fn: String): Unit = save(sc.hadoopConfiguration, new Path(fn))
  def save(sc: SparkContext, path: Path): Unit = save(sc.hadoopConfiguration, path)
  def save(hc: Configuration, fn: String): Unit = save(hc, new Path(fn))
  def save(hc: Configuration, path: Path): Unit = save(hc, path, overwrite = true)

  def save(sc: SparkContext, fn: String, overwrite: Boolean): Unit = save(sc.hadoopConfiguration, new Path(fn), overwrite)
  def save(sc: SparkContext, path: Path, overwrite: Boolean): Unit = save(sc.hadoopConfiguration, path, overwrite)
  def save(hc: Configuration, fn: String, overwrite: Boolean): Unit = save(hc, new Path(fn), overwrite)
  def save(hc: Configuration, path: Path, overwrite: Boolean): Unit = {
    val fs = FileSystem.get(hc)
    if (!fs.exists(path) || overwrite) {
      val os = fs.create(path)
      save(os)
      os.close()
    } else {
      logger.info(s"Skipping writing: $path")
    }
  }
}
