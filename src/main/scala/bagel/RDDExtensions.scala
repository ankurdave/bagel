package bagel

import spark._
import spark.SparkContext._

import scala.collection.mutable.ArrayBuffer

class RDDExtensions[T](self: RDD[T]) {
  def exists(p: T => Boolean): Boolean = {
    self.map(p).reduce(_ || _)
  }

  def forall(p: T => Boolean): Boolean = {
    self.map(p).reduce(_ && _)
  }
}

@serializable class PairRDDExtensions[K, V](self: RDD[(K, V)]) {
  def groupByKeyAsymmetrical[W, C](other: RDD[(K, W)], combiner: (C, W) => C, defaultCombined: () => C, mergeCombined: (C, C) => C, numSplits: Int): RDD[(K, (Option[V], C))] = {
    val vs: RDD[(K, Either[V, W])] = self.map { case (k, v) => (k, Left(v)) }
    val ws: RDD[(K, Either[V, W])] = other.map { case (k, w) => (k, Right(w)) }

    def createCombiner(a: Either[V, W]): (Option[V], C) = a match {
      case Left(v) => (Some(v), defaultCombined())
      case Right(w) => (None, combiner(defaultCombined(), w))
    }

    def mergeCombiners(a: (Option[V], C), b: (Option[V], C)): (Option[V], C) = {
      val (v1, c1) = a
      val (v2, c2) = b
      val v = v1.orElse(v2.orElse(None))
      (v, mergeCombined(c1, c2))
    }

    def mergeValue(a: (Option[V], C), b: Either[V, W]): (Option[V], C) = b match {
      case Left(v) => (Some(v), a._2)
      case Right(w) => (a._1, combiner(a._2, w))
    }

    (vs ++ ws).combineByKey(createCombiner, mergeValue, mergeCombiners, numSplits)
  }
}
