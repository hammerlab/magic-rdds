package org.hammerlab

import org.apache.hadoop.fs.FileSystem

package object hadoop {
  def disableLocalFSChecksums(implicit conf: Configuration = Configuration()): Unit = {
    val fs = FileSystem.getLocal(conf)
    fs.setVerifyChecksum(false)
    fs.setWriteChecksum(false)
  }
}
