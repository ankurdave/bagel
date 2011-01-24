import spark._
import spark.SparkContext._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

class Pregel[V, M, E]() {
  implicit def RDDExtensions[T](self: RDD[T]) = new RDDExtensions(self)
  implicit def PairRDDExtensions[K, V](self: RDD[(K, V)]) = new PairRDDExtensions(self)
  
  private val startVertices: HashMap[String, Vertex[V, E]] = new HashMap()
  private val startMessages: ListBuffer[Message[M]] = new ListBuffer()

  def addVertex(vertex: Vertex[V, E]) {
    startVertices.update(vertex.id, vertex)
  }

  def addEdge(edge: Edge[E], vertexId: String, default: String => Vertex[V,E]) {
    startVertices.update(vertexId, startVertices.get(vertexId) match {
      case Some(vertex) => vertex.copy(outEdges = edge +: vertex.outEdges)
      case None => default(vertexId)
    })
  }

  def addMessage(message: Message[M]) {
    startMessages += message
  }

  def run(sc: SparkContext)(compute: (Vertex[V,E], Iterable[Message[M]], Int) => (Vertex[V,E], Iterable[Message[M]])): RDD[Vertex[V,E]] =
    run(sc.parallelize(startVertices.values.toSeq), sc.parallelize(startMessages))(compute)

  def run(vertices: RDD[Vertex[V, E]], sc: SparkContext)(compute: (Vertex[V,E], Iterable[Message[M]], Int) => (Vertex[V,E], Iterable[Message[M]])): RDD[Vertex[V,E]] =
    run(vertices, sc.parallelize(startMessages))(compute)

  def run(vertices: RDD[Vertex[V,E]], messages: RDD[Message[M]], superstep: Int = 0)(
    compute: (Vertex[V,E], Iterable[Message[M]], Int) =>
      (Vertex[V,E], Iterable[Message[M]])): RDD[Vertex[V,E]] = {
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
            List(compute(vertex, ms, superstep))
        }
      }
    }.cache

    val newVertices = processed.map(_._1)
    val newMessages = processed.map(_._2).flatMap(identity)

    if (newMessages.count == 0 && newVertices.forall(_.state == Inactive))
      newVertices
    else
      run(newVertices, newMessages, superstep + 1)(compute)
  }
}

case class Message[A](targetId: String, value: A)
case class Vertex[A,B](id: String, value: A, outEdges: Seq[Edge[B]], state: VertexState)

case class Edge[A](targetId: String, value: A) {
  def messageAlong[B](messageValue: B) = Message(targetId, messageValue)
}

sealed abstract class VertexState
case object Active extends VertexState
case object Inactive extends VertexState
