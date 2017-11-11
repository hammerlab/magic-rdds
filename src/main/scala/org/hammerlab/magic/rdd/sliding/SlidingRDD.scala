package org.hammerlab.magic.rdd.sliding

import hammerlab.iterator._
import magic_rdds._
import org.apache.spark.rdd.RDD
import org.hammerlab.kryo._
import org.hammerlab.spark.PartitionIndex

import scala.reflect.ClassTag

/**
 * Helpers for mimicking Scala collections' "sliding" API on [[RDD]]s; iterates over successive N-element subsequences
 * of the [[RDD]].
 */
trait Sliding {
  implicit class SlidingOps[T: ClassTag](rdd: RDD[T]) extends Serializable {

    /**
     * For each input element, emit a pair that includes its successor.
     *
     * The last element of [[rdd]], which doesn't have a successor, is effectively dropped.
     *
     * Works in the presence of partitions of any size, including empty.
     */
    def sliding2: RDD[(T, T)] =
      sliding2Next
      .flatMap {
          case (elem, Some(next)) ⇒
            Some(elem → next)
          case _ ⇒
            None
        }

    /**
     * For each input element, emit a pair that includes its immediate successor, if it exists.
     *
     * Emits one tuple for each element in [[rdd]]; works in the presence of partitions of any size, including empty.
     */
    def sliding2Next: RDD[(T, Option[T])] =
      window(0, 1).map {
        case Window(_, elem, next) ⇒
          elem → next.headOption
      }

    def sliding2(pad: T): RDD[(T, T)] =
      sliding2Next.mapValues(_.getOrElse(pad))

    def sliding2Prev: RDD[(Option[T], T)] =
      window(1, 0).map {
        case Window(prev, elem, _) ⇒
          prev.headOption → elem
      }

    /**
     * For each input element, emit a triple that includes its two successors.
     *
     * The last two elements of [[rdd]], which don't have two successors, are effectively dropped.
     *
     * Works in the presence of partitions of any size, including empty.
     */
    def sliding3: RDD[(T, T, T)] =
      sliding3Next
      .flatMap {
          case (cur, Some(next1), Some(next2)) ⇒
            Some((cur, next1, next2))
          case _ ⇒
            None
        }

    /**
     * For each input element, emit a triple that includes its immediate predecessor and successor, if they exist.
     *
     * Emits one triplet for each element in [[rdd]]; works in the presence of partitions of any size, including empty.
     */
    def sliding3Opt: RDD[(Option[T], T, Option[T])] =
      window(1, 1).map {
        case Window(prev, elem, next) ⇒
          (
            prev.headOption,
            elem,
            next.headOption
          )
      }

    /**
     * For each input element, emit a triple that includes its two successors, if they exist.
     *
     * Emits one triplet for each element in [[rdd]]; works in the presence of partitions of any size, including empty.
     */
    def sliding3Next: RDD[(T, Option[T], Option[T])] =
      window(0, 2).map {
        case Window(_, elem, next) ⇒
          (
            elem,
            next.headOption,
            next.drop(1).headOption
          )
      }

    /**
     * Expose a sliding window over [[rdd]]: for each element in [[rdd]], a [[Seq]] is emitted with that element and its
     * `n-1` successors.
     *
     * Works even in the presence of partitions with fewer than `n` elements (including 0).
     *
     * @param n Total number of elements in each emitted sequence
     * @param includePartial if true, emit one [[Seq]] for each element in [[rdd]]; when false, the last `n-1` elements
     *                       (which don't have a full complement of `n-1` successors) will not "anchor" (be in the first
     *                       position of) any emitted [[Seq]]s
     */
    def sliding(n: Int, includePartial: Boolean = false): RDD[Seq[T]] =
      window(0, n - 1).flatMap {
        case Window(_, elem, next) ⇒
          if (!includePartial && next.size < n - 1)
            None
          else
            Some(Seq(elem) ++ next)
      }

    type Window = org.hammerlab.magic.rdd.sliding.Window[T]

    /**
     * Returns an [[RDD]] of tuples where every element from [[rdd]] appears once in the middle ("anchor") position of the
     * tuple, flanked by the preceding `numPrev` and succeeding `numNext` elements; if either of the flanking sequences
     * are incomplete, they are still included, but will be shorter than `numPrev` (resp. `numNext`) as appropriate.
     *
     * Works correctly in the presence of partitions of any size.
     */
    def window(numPrev: Int, numNext: Int): RDD[Window] = {

      val n = numPrev + 1 + numNext

      val N = rdd.getNumPartitions
      val sc = rdd.sparkContext

      val tooShortPartitions: Map[PartitionIndex, Int] =
        rdd
          .mapPartitionsWithIndex {
            (idx, it) ⇒
              val num = it.take(n - 1).size
              if (num < n - 1)
                Iterator(idx → num)
              else
                Iterator()
          }
          .collectAsMap()
          .toMap

      val tooShortPartitionsBroadcast =
        sc.broadcast(tooShortPartitions)

      val shiftedElems =
        rdd
          .mapPartitionsWithIndex(
            (partitionIdx, it) ⇒
              if (partitionIdx == 0)
                Iterator()
              else {
                val tooShortPartitions = tooShortPartitionsBroadcast.value
                var partitionCutoffs: List[(PartitionIndex, Int)] = Nil
                var sendToPartition = partitionIdx - 1
                var remainingElems = n - 1
                while (remainingElems > 0 && sendToPartition >= 0) {
                  val numStoppingAtCurPartition = tooShortPartitions.getOrElse(sendToPartition, n - 1)
                  if (numStoppingAtCurPartition > 0) {
                    partitionCutoffs = (sendToPartition, remainingElems) :: partitionCutoffs
                    remainingElems -= numStoppingAtCurPartition
                  }
                  sendToPartition -= 1
                }

                val prefix =
                  it
                    .take(n - 1)
                    .zipWithIndex
                    .buffered

                new SimpleIterator[((PartitionIndex, (PartitionIndex, Int)), T)] {

                  var nextElems: List[((PartitionIndex, (PartitionIndex, Int)), T)] = Nil

                  override protected def _advance: Option[((PartitionIndex, (PartitionIndex, PartitionIndex)), T)] =
                    nextElems match {
                      case Nil ⇒
                        prefix
                          .nextOption
                          .flatMap {
                            case (elem, idx) ⇒
                              partitionCutoffs.headOption foreach {
                                case (_, curCutoff)
                                  if (idx >= curCutoff) ⇒
                                  partitionCutoffs = partitionCutoffs.tail
                                case _ ⇒
                              }

                              nextElems =
                                for {
                                  (partition, _) ← partitionCutoffs
                                } yield
                                  partition →
                                    (partitionIdx → idx) →
                                    elem

                              if (nextElems.isEmpty)
                                None
                              else
                                _advance
                          }
                      case head :: rest ⇒
                        nextElems = rest
                        Some(head)
                    }
                }
              },
            preservesPartitioning = true
          )
          .partitionByKey(N)

      val prependElemsPartition =
        (0 until N)
          .find(
            !tooShortPartitions
              .get(_)
              .exists(_ == 0)
          )
          .getOrElse(N)

      rdd
        .zipPartitionsWithIndex(shiftedElems) {
          (idx, it, tailIter) ⇒

            val tail = tailIter.toList

            val slid: BufferedIterator[Seq[T]] =
              (it ++ tail)
                .slide(n)
                // Emitted elements should correspond exactly to each partition's extant elements; "tail" elements are
                // only used as succeeding context.
                .dropright(tail.size)
                .buffered

            val extraBeginElems =
              if (slid.hasNext && idx == prependElemsPartition) {
                var rest = slid.head
                var prev = Vector[T]()
                for {
                  _ ← (0 until numPrev).iterator
                } yield {
                  val elem = rest.head
                  rest = rest.tail
                  val window =
                    Window(
                      prev,
                      elem,
                      rest.view(0, numNext)
                    )
                  prev = prev :+ elem
                  window
                }
              } else
                Iterator()

            extraBeginElems ++
              new SimpleIterator[Window] {
                override protected def _advance: Option[Window] = {
                  slid
                    .nextOption
                    .flatMap {
                      next ⇒
                        val prev = next.take(numPrev)
                        val rest = next.drop(numPrev)
                        rest
                          .headOption
                          .map(
                            elem ⇒
                              Window(
                                prev,
                                elem,
                                rest.tail
                              )
                          )
                    }
                }
              }
        }
    }
  }
}

case class Window[T](prev: Seq[T], elem: T, next: Seq[T])

object Sliding
  extends spark.Registrar(
    // Used in BorrowElemsRDD's partitionOverridesBroadcast.
    cls[Map[Int, Int]],
    "scala.collection.immutable.Map$EmptyMap$"
  )
  with Sliding
