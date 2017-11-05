package org.hammerlab.magic.rdd.ordered

import hammerlab.iterator._
import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.ordered.SortedRDD.Bounds

import scala.reflect.ClassTag

trait SortedRepartition {
  implicit class SortedRepartitionOps[T: ClassTag](before: SortedRDD[T]) extends Serializable {

    private implicit val ord = before.ord

    def sortedRepartition(after: SortedRDD[T]): SortedRDD[T] = sortedRepartition(after.bounds)

    def sortedRepartition(newBounds: Bounds[T]): SortedRDD[T] =
      RangePartitionRDD(
        before.rdd,
        {
          val bounds =
            before
              .bounds
              .iterator
              .zipWithIndex
              .map(_.swap)
              .flatMap {
                case (idx, boundOpt) ⇒
                  boundOpt.map {
                    case (start, endOpt) ⇒
                      idx -> (start, endOpt)
                  }
              }
              .buffered

          val ord = before.ord

          newBounds
          .map {
            _.map {
              case (start, endOpt) ⇒
                bounds
                  .dropwhile {
                    case (_, (_, endOpt)) ⇒
                      endOpt.exists(ord.lteq(_, start))
                  }

                val endsBefore =
                  bounds
                    .collectwhile {
                      case (parentIdx, (_, parentEndOpt))
                        if endOpt.forall(
                          end ⇒
                            parentEndOpt.exists(
                              ord.lteq(_, end)
                            )
                        ) ⇒
                        parentIdx
                    }
                    .toVector

                bounds
                  .headOption match {
                    case Some((parentIdx, (parentStart, _)))
                      if endOpt.forall(
                        ord.lteq(parentStart, _)
                      ) ⇒
                      endsBefore :+ parentIdx
                    case _ ⇒
                      endsBefore
                  }
            }
          }
        },
        newBounds
      )

    def sortedPartitionZip[U: ClassTag](after: SortedRDD[T],
                                        fn: (Iterator[T], Iterator[T]) ⇒ Iterator[U]): RDD[U] =
      sortedRepartition(after)
        .rdd
        .zipPartitions(after.rdd)(fn)

  }
}
