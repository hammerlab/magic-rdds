package org.hammerlab

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable

package object collection {

  /**
   * [[CanBuildFrom]] instance for constructing [[Array]]s, not provided in standard library.
   */
  implicit def canBuildArray[From] =
    new CanBuildFrom[From, String, Array[String]] {
      override def apply(from: From): mutable.Builder[String, Array[String]] =
        mutable.ArrayBuilder.make[String]

      override def apply(): mutable.Builder[String, Array[String]] =
        mutable.ArrayBuilder.make[String]
    }

}
