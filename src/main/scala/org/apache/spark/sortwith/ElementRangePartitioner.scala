package org.apache.spark.sortwith

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}
import java.util
import java.util.Comparator

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.random.SamplingUtils
import org.apache.spark.util.{CollectionsUtils, Utils}
import org.apache.spark.{Partitioner, SparkEnv}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.hashing.byteswap32

class ElementRangePartitioner[T: ClassTag](@transient partitions: Int,
                                           @transient rdd: RDD[T],
                                           private var cmpFn: (T, T) => Int,
                                           private var ascending: Boolean = true)
  extends Partitioner {

  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")

  // An array of upper bounds for the first (partitions - 1) partitions
  private var rangeBounds: Array[T] = {
    if (partitions <= 1) {
      Array.empty
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      val sampleSize = math.min(20.0 * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      val (numItems, sketched) = ElementRangePartitioner.sketch(rdd, sampleSizePerPartition)
      if (numItems == 0L) {
        Array.empty
      } else {
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(T, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach { case (idx, n, sample) =>
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx
          } else {
            // The weight is 1 over the sampling probability.
            val weight = (n.toDouble / sample.length).toFloat
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
        }
        if (imbalancedPartitions.nonEmpty) {
          // Re-sample imbalanced partitions with the desired sampling probability.
          val imbalanced = new PartitionPruningRDD(rdd, imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        ElementRangePartitioner.determineBounds[T](candidates, partitions, cmpFn)
      }
    }
  }

  def numPartitions: Int = rangeBounds.length + 1

  private var binarySearch: ((Array[T], T) => Int) = SortUtils.makeBinarySearch(cmpFn)

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[T]
    var partition = 0
    if (rangeBounds.length <= 128) {
      // If we have less than 128 partitions naive search
      while (partition < rangeBounds.length && cmpFn(k, rangeBounds(partition)) > 0) {
        partition += 1
      }
    } else {
      // Determine which binary search method to use only once.
      partition = binarySearch(rangeBounds, k)
      // binarySearch either returns the match location or -[insertion point]-1
      if (partition < 0) {
        partition = -partition-1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
    if (ascending) {
      partition
    } else {
      rangeBounds.length - partition
    }
  }

  override def equals(other: Any): Boolean = other match {
    case r: ElementRangePartitioner[_] =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending && r.cmpFn == cmpFn
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < rangeBounds.length) {
      result = prime * result + rangeBounds(i).hashCode
      i += 1
    }
    result = prime * result + ascending.hashCode + cmpFn.hashCode
    result
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeObject(cmpFn)
        out.writeBoolean(ascending)
        out.writeObject(binarySearch)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser) { stream =>
          stream.writeObject(scala.reflect.classTag[Array[T]])
          stream.writeObject(rangeBounds)
        }
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        cmpFn = in.readObject().asInstanceOf[(T, T) => Int]
        ascending = in.readBoolean()
        binarySearch = in.readObject().asInstanceOf[(Array[T], T) => Int]

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          implicit val classTag = ds.readObject[ClassTag[Array[T]]]()
          rangeBounds = ds.readObject[Array[T]]()
        }
    }
  }
}

private[spark] object ElementRangePartitioner {

  /**
    * Sketches the input RDD via reservoir sampling on each partition.
    *
    * @param rdd the input RDD to sketch
    * @param sampleSizePerPartition max sample size per partition
    * @return (total number of items, an array of (partitionId, number of items, sample))
    */
  def sketch[T : ClassTag](rdd: RDD[T],
                           sampleSizePerPartition: Int): (Long, Array[(Int, Long, Array[T])]) = {
    val shift = rdd.id
    // val classTagK = classTag[K] // to avoid serializing the entire partitioner object
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n) = SamplingUtils.reservoirSampleAndCount(
        iter, sampleSizePerPartition, seed)
      Iterator((idx, n, sample))
    }.collect()
    val numItems = sketched.map(_._2.toLong).sum
    (numItems, sketched)
  }

  /**
    * Determines the bounds for range partitioning from candidates with weights indicating how many
    * items each represents. Usually this is 1 over the probability used to sample this candidate.
    *
    * @param candidates unordered candidates with weights
    * @param partitions number of partitions
    * @return selected bounds
    */
  def determineBounds[T: ClassTag](candidates: ArrayBuffer[(T, Float)],
                                   partitions: Int,
                                   cmpFn: (T, T) => Int): Array[T] = {
    val ordered =
      try {
        candidates.sortWith((p1, p2) => cmpFn(p1._1, p2._1) < 0)
      } catch {
        case e: IllegalArgumentException =>
          throw new IllegalArgumentException(
            s"Inconsistent sort:\n\t${candidates.take(1000).map(_._1).map({
              case ((t1, t2, t3), idx) => s"($t1,$t2,$t3) -> $idx"
              case o => o.toString
            }).mkString("\n\t")}",
            e
          )
      }
    val numCandidates = ordered.size
    val sumWeights = ordered.map(_._2.toDouble).sum
    val step = sumWeights / partitions
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[T]
    var i = 0
    var j = 0
    var previousBound = Option.empty[T]
    while ((i < numCandidates) && (j < partitions - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight > target) {
        // Skip duplicate values.
        if (previousBound.isEmpty || cmpFn(key, previousBound.get) > 0) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }
    bounds.toArray
  }
}
