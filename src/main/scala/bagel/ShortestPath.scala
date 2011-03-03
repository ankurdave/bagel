package bagel

import spark._
import spark.SparkContext._

object ShortestPath {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: ShortestPath <graphFile> <startVertex> " +
                         "<numSplits> <host>")
      System.exit(-1)
    }

    val graphFile = args(0)
    val startVertex = args(1)
    val numSplits = args(2).toInt
    val host = args(3)
    val sc = new SparkContext(host, "ShortestPath")

    // Parse the graph data from a file into two RDDs, vertices and messages
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
               new SPEdge(targetId, edgeValue.toInt)
           }
           
           new SPVertex(vertexId, Int.MaxValue, outEdges, Active)
         }
       })

    val messages =
      (lines.filter(_.length == 2)
       .map {
         case Array(vertexId, messageValue) =>
           new SPMessage(vertexId, messageValue.toInt)
       })
    
    System.err.println("Read "+vertices.count()+" vertices and "+
                       messages.count()+" messages.")

    // Do the computation
    def messageCombiner(minSoFar: Int, message: SPMessage): Int =
      Math.min(minSoFar, message.value)

    val result = Pregel.run(vertices, messages, numSplits, messageCombiner, Int.MaxValue, Math.min _) {
      (self: SPVertex, messageMinValue: Int, superstep: Int) =>
        val newValue = Math.min(self.value, messageMinValue)

        val outbox =
          if (newValue != self.value)
            self.outEdges.map(edge =>
              new SPMessage(edge.targetId, newValue + edge.value))
          else
            List()

        (new SPVertex(self.id, newValue, self.outEdges, Inactive), outbox)
    }

    // Print the result
    System.err.println("Shortest path from "+startVertex+" to all vertices:")
    val shortest = result.map(vertex =>
      "%s\t%s\n".format(vertex.id, vertex.value match {
        case x if x == Int.MaxValue => "inf"
        case x => x
      })).collect.mkString
    println(shortest)
  }
}

@serializable class SPVertex(val id: String, val value: Int, val outEdges: Seq[SPEdge], val state: VertexState) extends Vertex
@serializable class SPEdge(val targetId: String, val value: Int) extends Edge
@serializable class SPMessage(val targetId: String, val value: Int) extends Message
