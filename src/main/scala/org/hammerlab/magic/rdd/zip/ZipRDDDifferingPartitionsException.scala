package org.hammerlab.magic.rdd.zip

import org.apache.spark.rdd.RDD

case class ZipRDDDifferingPartitionsException(rdds: Seq[RDD[_]])
  extends Exception(
    (
      Seq("Attempting to zip RDDs with differing numbers of partitions:") ++
      rdds.map(rdd ⇒ s"$rdd (${rdd.id}): ${rdd.getNumPartitions}")
    )
    .mkString("\n\t")
  )
