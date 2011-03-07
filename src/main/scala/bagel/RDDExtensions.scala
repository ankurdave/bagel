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

@serializable class PairRDDExtensions[K, V, W](self: RDD[(K, Either[V, W])]) {
  def groupByKeyAsymmetrical[C](combiner: (C, W) => C, defaultCombined: () => C, mergeCombined: (C, C) => C, numSplits: Int): RDD[(K, (Option[V], C))] = {
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

    self.combineByKey(createCombiner, mergeValue, mergeCombiners, numSplits)
  }
}
