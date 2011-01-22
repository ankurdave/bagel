import spark._
import spark.SparkContext._

object Pregel {
  implicit def RDDExtensions[T](self: RDD[T]) = new RDDExtensions(self)
  implicit def PairRDDExtensions[K,V](self: RDD[(K, V)]) = new PairRDDExtensions(self)

  def run[V,M,E](vertices: RDD[Vertex[V,E]], messages: RDD[Message[M]])(
    compute: (Vertex[V,E], Iterable[Message[M]]) =>
      (Vertex[V,E], Iterable[Message[M]])): RDD[Vertex[V,E]] = {
    println("Vertices:\n" + 
            vertices.map("\t" + _.toString).collect.mkString("\n") + "\n" +
            "Messages:\n" +
            messages.map("\n" + _.toString).collect.mkString("\n") + "\n")
    
    // Console.readLine("Press Enter... ")

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
    }.cache

    val newVertices = processed.map(_._1)
    val newMessages = processed.map(_._2).flatMap(identity)

    if (newMessages.count == 0 && newVertices.forall(_.state == Inactive))
      newVertices
    else
      run(newVertices, newMessages)(compute)
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
