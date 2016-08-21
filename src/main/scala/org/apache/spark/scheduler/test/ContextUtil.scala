package org.apache.spark.scheduler.test

import org.apache.spark.SparkContext

/**
 * Package-cheat to expose the number of Spark jobs that have been run, for testing purposes.
 */
trait ContextUtil {
  def numJobs()(implicit sc: SparkContext): Int =
    sc.dagScheduler.nextJobId.get()
}
