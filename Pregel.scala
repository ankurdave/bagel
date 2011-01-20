object Pregel {
  def run[VertexValue, MessageValue, EdgeValue](graph: Iterable[Either[Vertex[VertexValue, EdgeValue], Message[MessageValue]]], compute: (Vertex[VertexValue, EdgeValue], Iterable[Message[MessageValue]]) => Iterable[Either[Vertex[VertexValue, EdgeValue], Message[MessageValue]]]): Iterable[Vertex[VertexValue, EdgeValue]] = {
    val newGraph = 
      (graph
       .groupBy {
         case Left(vertex) => vertex.id
         case Right(message) => message.targetId }
       .flatMap {
         case (vertexId, vertexRelated) => {
           vertexRelated.find { case Left(_) => true; case Right(_) => false } match {
             case Some(Left(vertex)) => {
               val messages = vertexRelated.flatMap { case Left(vertex) => List(); case Right(message) => List(message) }

               if (messages.isEmpty && (vertex.state match { case Inactive => true; case _ => false }))
                 List(Left(vertex))
               else
                 compute(vertex, messages)
             }
             case Some(Right(message)) => throw new Exception("Somehow a Message got past the call to find above")
             case None => List() }}})

    val terminate =
      (!newGraph.exists { case Left(_) => false; case Right(_) => true } &&
       newGraph.forall {
         case Left(vertex) =>
           vertex.state match { case Inactive => true; case _ => false }
         case Right(message) => true })
        
    if (terminate)
      newGraph.flatMap { case Left(vertex) => List(vertex); case Right(message) => List() }
    else
      run(newGraph, compute)
  }
}

case class Edge[EdgeValue](targetId: String, value: EdgeValue) {
  def messageAlong[MessageValue](messageValue: MessageValue) = Message(targetId, messageValue)
}

case class Message[MessageValue](targetId: String, value: MessageValue)

case class Vertex[VertexValue, EdgeValue](id: String, value: VertexValue, outEdges: Iterable[Edge[EdgeValue]], state: VertexState)

sealed abstract class VertexState
case object Active extends VertexState
case object Inactive extends VertexState
