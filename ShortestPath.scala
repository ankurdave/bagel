import spark._
import spark.SparkContext._

object ShortestPath {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: ShortestPath <graphFile> <startVertex> <host>")
      System.exit(-1)
    }

    val graphFile = args(0)
    val startVertex = args(1)
    val host = args(2)

    val sc = new SparkContext(host, "ShortestPath")

    // Parse the graph data from a file into an RDD
    val graph: RDD[Either[Vertex[Option[Int], Int], Message[Int]]] = 
      (sc
       .textFile(graphFile)
       .filter(line => !line.matches("^\\s*#.*"))
       .map(line => line.split("\t"))
       .groupBy(line => line(0))
       .map {
         case (vertexId, lines) => {
           val outEdges = lines.map(
             line => {
               Edge(line(1), line(2).toInt) })
           Left(Vertex(vertexId, None, outEdges, Active)) }})
    System.err.println("Read " + graph.count() + " vertices.")

    // Do the computation
    def compute(self: Vertex[Option[Int], Int], messages: Iterable[Message[Int]]): Iterable[Either[Vertex[Option[Int], Int], Message[Int]]] = {
      val newValue = (self.value.getOrElse(Int.MaxValue) :: messages.map(_.value).toList).min
      
      val outbox =
        if (newValue != self.value)
          self.outEdges.map(edge => Right(edge.messageAlong(newValue + edge.value))).toList
        else
          List()
      
      Left[Vertex[Option[Int],Int],Message[Int]](Vertex(self.id, Some(newValue), self.outEdges, Inactive)) :: outbox
    }

    val result = Pregel.run(graph ++ Right(Message(startVertex, 0)), compute)
    val shortest = result.map(vertex => "%s\t%s\n".format(vertex.id, vertex.value.getOrElse("inf"))).mkString

    System.err.println("Shortest path from "+startVertex+" to all vertices:")
    println(shortest)
  }
}
