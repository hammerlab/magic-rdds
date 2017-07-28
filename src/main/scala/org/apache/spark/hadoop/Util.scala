package org.apache.spark.hadoop

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.InputMetrics

object Util {
  def getFSBytesReadOnThreadCallback: () => Long =
    SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()

  def setBytesRead(bytesRead: Long)(implicit inputMetrics: InputMetrics): Unit =
    inputMetrics.setBytesRead(bytesRead)

  def incRecordsRead(amount: Long = 1)(implicit inputMetrics: InputMetrics): Unit =
    inputMetrics.incRecordsRead(amount)

  def incBytesRead(amount: Long = 1)(implicit inputMetrics: InputMetrics): Unit =
    inputMetrics.incBytesRead(amount)

  val UPDATE_INPUT_METRICS_INTERVAL_RECORDS = SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS
}
