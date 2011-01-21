import spark._
import spark.SparkContext._

import scala.collection.mutable.ArrayBuffer


object Pregel {
  implicit def RDDExtensions[T](self: RDD[T]) = new RDDExtensions(self)
  implicit def PairRDDExtensions[K,V](self: RDD[(K, V)]) = new PairRDDExtensions(self)

  def run[V,M,E](vertices: RDD[Vertex[V,E]], messages: RDD[Message[M]])(compute: (Vertex[V,E], Seq[Message[M]]) => (Vertex[V,E], Seq[Message[M]])): Seq[Vertex[V,E]] = {
    println("Vertices:")
    println(vertices.map(_.toString).collect.mkString("\n"))
    println("Messages:")
    println(messages.map(_.toString).collect.mkString("\n"))
    Console.readLine("Press Enter... ")

    val verticesWithId = vertices.map(v => (v.id, v))
    val messagesWithId = messages.map(m => (m.targetId, m))
    val joined = verticesWithId outerJoin messagesWithId

    val processed = joined.flatMap {
      case (vertexId, (vs, ms)) => {
        if (vs.isEmpty) {
          List()
        } else if (vs.length > 1) {
          throw new Exception("Two vertices with the same ID: "+vertexId)
        } else {
          val vertex = vs.head
          if (ms.isEmpty && vertex.state == Inactive)
            List((vertex, ms))
          else
            List(compute(vertex, ms))
        }
      }
    }

    val (newVertices, newMessages) = (processed.map(_._1), processed.map(_._2))
    val newM = newMessages.flatMap(identity)

    if (newM.count == 0 && newVertices.forall(_.state == Inactive))
      newVertices.collect
    else
      run(newVertices, newM)(compute)
  }
}

case class Message[A](targetId: String, value: A)
case class Vertex[A,B](id: String, value: A, outEdges: Iterable[Edge[B]], state: VertexState)

case class Edge[A](targetId: String, value: A) {
  def messageAlong[B](messageValue: B) = Message(targetId, messageValue)
}

sealed abstract class VertexState
case object Active extends VertexState
case object Inactive extends VertexState

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
