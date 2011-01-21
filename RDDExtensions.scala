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
  // Differs from the standard join in two ways: (1) a pair from one
  // RDD is included even if there's no corresponding pair in the
  // other RDD, and (2) multiple values are grouped together in a Seq
  // rather than forming the Cartesian product.
  def outerJoin[W](other: RDD[(K, W)], numSplits: Int): RDD[(K, (Seq[V], Seq[W]))] = {
    val vs: RDD[(K, Either[V, W])] = self.map { case (k, v) => (k, Left(v)) }
    val ws: RDD[(K, Either[V, W])] = other.map { case (k, w) => (k, Right(w)) }
    (vs ++ ws).groupByKey(numSplits).map {
      case (k, seq) => {
        val vbuf = new ArrayBuffer[V]
        val wbuf = new ArrayBuffer[W]
        seq.foreach(_ match {
          case Left(v) => vbuf += v
          case Right(w) => wbuf += w
        })
        (k, (vbuf.toList, wbuf.toList))
      }
    }
  }

  def outerJoin[W](other: RDD[(K, W)]): RDD[(K, (Seq[V], Seq[W]))] = {
    outerJoin(other, numCores)
  }

  def numCores = self.sparkContext.numCores
}
