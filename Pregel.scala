import spark._

object Pregel {
  implicit def RDDExtensions[T](rdd: RDD[T]) = new RDDExtensions(rdd)

  def run[V,M,E](graph: RDD[GraphObject])(compute: (Vertex[V,E], Iterable[Message[M]]) => Iterable[GraphObject]): Iterable[Vertex[V,E]] = {
    println("Processing graph:")
    println(graph.map(_.toString).collect.mkString("\n"))
    Console.readLine("Press enter... ")

    val grouped = graph.groupBy {
      case v: Vertex[_,_] => v.id
      case m: Message[_] => m.targetId
    }

    val processed = grouped.flatMap {
      case (vertexId, related) =>
        related.find { case _: Vertex[_,_] => true; case _ => false } match {
          case Some(v: Vertex[_,_]) => {
            val vertex = v.asInstanceOf[Vertex[V,E]]

            val messages = (related.collect {
              case m: Message[_] => m.asInstanceOf[Message[M]]
            })
            
            if (messages.isEmpty && vertex.state == Inactive)
              List(vertex)
            else
              compute(vertex, messages)
          }
          
          case Some(_) =>
            throw new Exception("Not a vertex")
          
          case None => List()
        }
    }

    val messagesMoving = processed.exists {
      case _: Message[_] => true
      case _ => false
    }
    val allVerticesInactive = processed.forall {
      case v: Vertex[_,_] => v.state == Inactive
      case _ => true
    }
        
    if (!messagesMoving && allVerticesInactive)
      processed.flatMap {
        case v: Vertex[_,_] => List(v.asInstanceOf[Vertex[V,E]])
        case _ => List()
      }.collect
    else
      run(processed)(compute)
  }
}

case class Edge[A](targetId: String, value: A) {
  def messageAlong[B](messageValue: B) = Message(targetId, messageValue)
}

sealed trait GraphObject
case class Message[A](targetId: String, value: A) extends GraphObject
case class Vertex[A,B](id: String, value: A, outEdges: Iterable[Edge[B]], state: VertexState) extends GraphObject

sealed abstract class VertexState
case object Active extends VertexState
case object Inactive extends VertexState

class RDDExtensions[T](rdd: RDD[T]) {
  def exists(p: T => Boolean): Boolean = {
    rdd.map(p).reduce(_ || _)
  }

  def forall(p: T => Boolean): Boolean = {
    rdd.map(p).reduce(_ && _)
  }
}
