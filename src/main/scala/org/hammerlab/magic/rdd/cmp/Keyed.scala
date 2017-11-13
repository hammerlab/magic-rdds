package org.hammerlab.magic.rdd.cmp

import hammerlab.monoid._
import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.cmp.Keyed.Stats

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Given an outer-join of two [[RDD]]s, expose statistics about how many identical elements at identical positions they
 * have.
 */
class Keyed[K: ClassTag, V: ClassTag] private(cogrouped: RDD[(K, (Iterable[V], Iterable[V]))])
  extends Serializable {

  lazy val keyCmps =
    cogrouped
      .mapValues {
        case (as, bs) ⇒
          val map = mutable.Map[V, (Int, Int)]()

          as
            .foreach {
              a ⇒
                map
                  .update(
                    a,
                    map.getOrElse(a, (0, 0)) |+| (1, 0)
                  )
            }

          bs
            .foreach {
              b ⇒
                map
                  .update(
                    b,
                    map.getOrElse(b, (0, 0)) |+| (0, 1)
                  )
            }

          map
            .map {
              case (v, (na, nb)) ⇒
                if (na == 0)
                  Stats(onlyB = nb)
                else if (nb == 0)
                  Stats(onlyA = na)
                else if (na > nb)
                  Stats(eq = nb, extraA = na - nb)
                else
                  Stats(eq = na, extraB = nb - na)
            }
            .reduce(_ |+| _)
      }


  lazy val stats: Stats = keyCmps.values.reduce(_ |+| _)

  lazy val Stats(
    eq,
    extraA,
    extraB,
    onlyA,
    onlyB
  ) = stats
}

object Keyed extends Serializable {
  def apply[K: ClassTag, V: ClassTag](rdd1: RDD[(K, V)], rdd2: RDD[(K, V)]): Keyed[K, V] =
    new Keyed(
      rdd1.cogroup(rdd2)
    )

  /**
   * Counts of key-value pairs that are the same vs. different in two [[RDD]]s
   *
   * @param eq number of equal key-value pairs: maximum size of an injection from either RDD into the other
   * @param extraA left-side pairs also present on the right, but in fewer numbers
   * @param extraB right-side pairs also present on the left, but in fewer numbers
   * @param onlyA left-side pairs not present on the right
   * @param onlyB right-side pairs not present on the left
   */
  case class Stats(eq: Int = 0,
                   extraA: Int = 0,
                   extraB: Int = 0,
                   onlyA: Int = 0,
                   onlyB: Int = 0)
}
