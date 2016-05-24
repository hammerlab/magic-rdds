package org.hammerlab.magic.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class BorrowElemsRDD[T: ClassTag](@transient rdd: RDD[T]) extends Serializable {

  def shift(n: Int, fill: T): RDD[T] = shift(n, Some(fill))
  def shift(n: Int, fillOpt: Option[T] = None): RDD[T] = {
    copyN(
      n,
      (partitionIdx: Int, it: Iterator[T], arr: Array[T]) =>
        (
          if (partitionIdx == 0)
            it
          else
            it.drop(n)
        ) ++ arr.toIterator,
      fillOpt
    )
  }

  def copy(n: Int, fill: T): RDD[T] = copy(n, Some(fill))
  def copy(n: Int, fillOpt: Option[T] = None): RDD[T] = {
    copyN(
      n,
      (_: Int, it: Iterator[T], arr: Array[T]) => it ++ arr.toIterator,
      fillOpt
    )
  }

  def copyN(n: Int, fn: (Int, Iterator[T], Array[T]) => Iterator[T], fill: T): RDD[T] = copyN(n, fn, Some(fill))
  def copyN(n: Int, fn: (Int, Iterator[T], Array[T]) => Iterator[T], fillOpt: Option[T] = None): RDD[T] = {
    val N = rdd.getNumPartitions

    val firstSplit: RDD[(Int, Array[T])] =
      rdd.mapPartitionsWithIndex((idx, iter) =>
        if (idx == 0)
          fillOpt.map(fill => (N - 1) -> Array.fill(n)(fill)).toIterator
        else {
          val copiedElems = iter.take(n).toArray
          if (copiedElems.length < n) {
            throw new NoSuchElementException(
              s"Found only ${copiedElems.length} elements in partition $idx; needed ≥ $n"
            )
          }
          Iterator(
            (idx - 1) → copiedElems
          )
        }
      ).partitionBy(new KeyPartitioner(N))

    rdd.zipPartitions(firstSplit)((iter, tailIter) ⇒
      if (tailIter.hasNext) {
        val (partitionIdx, arr) = tailIter.next()
        fn(partitionIdx, iter, arr)
      } else
        fn(N - 1, iter, Array())
    )
  }

//  def copyOne(fn: (Iterator[T], Option[T]) => Iterator[T], fillOpt: Option[T] = None): RDD[T] =
//    copyN(
//      1,
//      (it: Iterator[T], arr: Array[T]) => fn(it, arr.headOption),
//      fillOpt
//    )
//
//  def copyOne(fn: (Iterator[T], T) => Iterator[T], fill: T): RDD[T] =
//    copyOne(
//      (it: Iterator[T], lastOpt: Option[T]) => fn(it, lastOpt.get),
//      Some(fill)
//    )
//
//  def copyTwo(fn: (Iterator[T], Option[(T, T)]) => Iterator[T], fillOpt: Option[T] = None): RDD[T] =
//    copyN(
//      2,
//      (it: Iterator[T], arr: Array[T]) =>
//        fn(
//          it,
//          if (arr.length >= 2)
//            Some((arr(0), arr(1)))
//          else
//            None
//        ),
//      fillOpt
//    )
//
//  def copyTwo(fn: (Iterator[T], (T, T)) => Iterator[T], fill: T): RDD[T] =
//    copyTwo(
//      (it: Iterator[T], lastOpt: Option[(T, T)]) =>
//        fn(it, lastOpt.get), Some(fill)
//    )

  def shiftPartitions[U: ClassTag]: RDD[T] =
    rdd
      .mapPartitionsWithIndex((partitionIdx, it) => {
        if (partitionIdx > 0) {
          for {
            (elem, idx) <- it.zipWithIndex
          } yield
            (partitionIdx - 1, idx) -> elem
        } else
          Iterator()
      })
      .repartitionAndSortWithinPartitions(KeyPartitioner(rdd))
      .values
}

object BorrowElemsRDD {
  implicit def rddToBorrowElemsRDD[T: ClassTag](rdd: RDD[T]): BorrowElemsRDD[T] = new BorrowElemsRDD(rdd)
}
