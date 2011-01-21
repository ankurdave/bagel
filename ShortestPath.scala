import spark._
import spark.SparkContext._

object ShortestPath {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: ShortestPath <graphFile> <startVertex> <host>")
      System.exit(-1)
    }

    val graphFile = args(0)
    val startVertex = args(1)
    val host = args(2)

    val sc = new SparkContext(host, "ShortestPath")

    // Parse the graph data from a file into an RDD
    val lines =
      (sc.textFile(graphFile)
       .filter(!_.matches("^\\s*#.*"))
       .map(line => line.split("\t")))

    val vertices =
      (lines.groupBy(line => line(0))
       .map {
         case (vertexId, lines) => {
           val outEdges = lines.collect {
             case Array(_, targetId, edgeValue) =>
               Edge(targetId, edgeValue.toInt)
           }
           
           Vertex[Option[Int],Int](vertexId, None, outEdges, Active)
         }
       })

    val messages =
      (lines.filter(_.length == 2)
       .map {
         case Array(vertexId, messageValue) =>
           Message(vertexId, messageValue.toInt)
       })
    
    System.err.println("Read "+vertices.count()+" vertices and "+messages.count()+" messages.")

    // Do the computation
    val result = Pregel.run(vertices, messages) { (self: Vertex[Option[Int],Int], messages: Seq[Message[Int]]) =>
      // TODO: Need newValue to be Option[Int], otherwise it just
      // takes on Some(Int.MaxValue) after the first iteration
      val newValue = (self.value.getOrElse(Int.MaxValue) :: messages.map(_.value).toList).min

      val outbox =
        if (newValue != self.value.getOrElse(Int.MaxValue))
          self.outEdges.map(edge => edge.messageAlong(newValue + edge.value)).toList
        else
          List()

      (Vertex(self.id, Some(newValue), self.outEdges, Inactive), outbox)
    }

    // Print the result
    System.err.println("Shortest path from "+startVertex+" to all vertices:")
    val shortest = result.map(vertex => "%s\t%s\n".format(vertex.id, vertex.value.getOrElse("inf"))).mkString
    println(shortest)
  }
}
