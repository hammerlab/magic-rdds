package org.hammerlab.magic.rdd.grid

/**
 * Messages sent between partitions representing partial-sums
 */
sealed trait Message[T]
case class BottomLeftElem[T](t: T) extends Message[T]
case class LeftCol[T](m: Map[Int, T]) extends Message[T]
case class BottomRow[T](m: Map[Int, T]) extends Message[T]
