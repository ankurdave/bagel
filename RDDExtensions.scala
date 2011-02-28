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

class PairRDDExtensions[K, V](self: RDD[(K, V)]) {
  def groupByKeyAsymmetrical[W, A](other: RDD[(K, W)], mergeOther: (A, W) => A, defaultOther: A, mergeMultipleOther: (A, A) => A, numSplits: Int): RDD[(K, (V, A))] = {
    val vs: RDD[(K, Either[V, W])] = self.map { case (k, v) => (k, Left(v)) }
    val ws: RDD[(K, Either[V, W])] = other.map { case (k, w) => (k, Right(w)) }

    def createCombiner(a: Either[V, W]) = a match {
      case Left(v) => (Some(v), defaultOther)
      case Right(w) => (None, mergeOther(defaultOther, w))
    }

    def mergeCombiners(b1: (Option[V], A), b2: (Option[V], A)) = {
      val (v1, ws1) = b1
      val (v2, ws2) = b2
      val v = v1.getOrElse(v2.getOrElse(None))
      (v, mergeMultipleOther(ws1, ws2))
    }

    def mergeValue(buf: (Option[V], A), a: Either[V, W]) = a match {
      case Left(v) => (v, buf._2)
      case Right(w) => (buf._1, mergeOther(buf._2, w))
    }

    (vs ++ ws).combineByKey(createCombiner, mergeValue, mergeCombiners, numSplits)
    }
  }

  def outerJoin[W](other: RDD[(K, W)]): RDD[(K, (Seq[V], Seq[W]))] = {
    outerJoin(other, numCores)
  }

  def numCores = self.sparkContext.numCores
}
