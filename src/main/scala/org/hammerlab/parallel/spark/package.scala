package org.hammerlab.parallel

import org.apache.spark.SparkContext

package object spark {
  def apply(strategy: PartitioningStrategy)(
      implicit sc: SparkContext
  ): spark.Config =
    spark
      .Config()(
        sc,
        strategy
      )

  def apply(
      implicit
      sc: SparkContext,
      strategy: PartitioningStrategy
  ): spark.Config =
      spark
        .Config()(
          sc,
          strategy
        )
}
