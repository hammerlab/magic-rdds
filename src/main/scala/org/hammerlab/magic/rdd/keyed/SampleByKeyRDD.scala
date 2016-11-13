package org.hammerlab.magic.rdd.keyed

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.rdd.RDD
import org.hammerlab.math.HypergeometricDistribution

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

class KeySamples[V](var num: Long, var vs: ArrayBuffer[V], max: Int) extends Serializable {

  /**
   * Private buffer in which we accumulate up to 2*max elements. Public accessor @values lazily samples this down to
   * `max` elements when appropriate.
   */
  private var _values: Option[ArrayBuffer[V]] = Some(vs)

  def values: ArrayBuffer[V] = {
    _values match {
      case Some(v) => v
      case None =>
        val vals = sample(max)
        _values = Some(vals)
        vals
    }
  }

  def sample(num: Int): ArrayBuffer[V] = {
    if (vs.length > num) {
      val v = ArrayBuffer[V]()
      val n = vs.length
      var remaining = num
      (0 until n).foreach(i => {
        val eligible = n - i
        val r = Random.nextInt(eligible)
        println(s"int: $r")
        if (r < remaining) {
          v += vs(i)
          remaining -= 1
        }
      })
      v
    } else {
      vs
    }
  }

  def ++=(o: KeySamples[V]): KeySamples[V] = {
    val finalNum = num + o.num

    val vals = values
    val oVals = o.values

    val finalNumSamples = math.min(vals.length + oVals.length, max)

    val hgd = HypergeometricDistribution(finalNum, num, finalNumSamples)

    val d = Random.nextDouble()
    println(s"double: $d")
    val selfNumToTake = hgd.invCDF(d)

    val otherNumToTake = finalNumSamples - selfNumToTake

    val elems = sample(selfNumToTake)
    elems ++= o.sample(otherNumToTake)

    new KeySamples(finalNum, elems, max)
  }

  def +=(v: V): KeySamples[V] = {
    num += 1
    vs += v

    // Invalidate cached _values.
    _values = None

    // Only recompute _values 1/max of the time, for amortized O(n) instead of O(n²).
    if (vs.length > 2 * max) {
      vs = values
    }
    this
  }
}

object KeySamples {
  def register(kryo: Kryo): Unit = {
    kryo.register(classOf[KeySamples[_]])
  }
}

class SampleByKeyRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
  def sampleByKey(numPerKey: Int): RDD[(K, ArrayBuffer[V])] =
    rdd
      .combineByKey[KeySamples[V]](
        (e: V) => new KeySamples(1, ArrayBuffer(e), numPerKey),
        (v: KeySamples[V], e: V) => v += e,
        (v1: KeySamples[V], v2: KeySamples[V]) => v1 ++= v2,
        rdd.getNumPartitions
      )
      .mapValues(_.values)
}

object SampleByKeyRDD{
  implicit def rddToSampleByKeyRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): SampleByKeyRDD[K, V] =
    new SampleByKeyRDD(rdd)
}
