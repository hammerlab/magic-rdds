package org.hammerlab

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * [[CanBuildFrom]] instances for constructing [[Array]]s and [[Vector]]s, not provided in standard library for some
 * reason.
 */
package object collection {
  implicit def canBuildArray[From, T: ClassTag] =
    new CanBuildFrom[From, T, Array[T]] {
      override def apply(from: From): mutable.Builder[T, Array[T]] =
        mutable.ArrayBuilder.make[T]

      override def apply(): mutable.Builder[T, Array[T]] =
        mutable.ArrayBuilder.make[T]
    }

  implicit def canBuildVector[From, T] =
    new CanBuildFrom[From, T, Vector[T]] {
      override def apply(from: From): mutable.Builder[T, Vector[T]] =
        Vector.newBuilder[T]

      override def apply(): mutable.Builder[T, Vector[T]] =
        Vector.newBuilder[T]
    }
}
