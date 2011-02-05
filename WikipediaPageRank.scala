import spark._
import spark.SparkContext._

import scala.xml.{XML,NodeSeq}

object WikipediaPageRank {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: PageRank <inputFile> <threshold> <numSplits> <host>")
      System.exit(-1)
    }

    val inputFile = args(0)
    val threshold = args(1).toDouble
    val numSplits = args(2).toInt
    val host = args(3)
    val sc = new SparkContext(host, "WikipediaPageRank")

    // Parse the Wikipedia page data into a graph
    val input = sc.textFile(inputFile)
    val numVertices = input.count()
    val vertices =
      input.map(line => {
        val fields = line.split("\t")
        val (title, body) = (fields(1), fields(3).replace("\\n", "\n"))
        val links =
          try {
            XML.loadString(body) \\ "link" \ "target"
          } catch {
            case e: org.xml.sax.SAXParseException => {
              System.err.println("Article \""+title+"\" has malformed XML in body:\n"+body)
              NodeSeq.Empty
            }
          }
        val outEdges = links.map(link => new PREdge(link.text))
        new PRVertex(title, 1.0 / numVertices, outEdges, Active)
      })

    // Do the computation
    val epsilon = 0.001 / numVertices
    val result = Pregel.run[PRVertex, PRMessage](vertices, sc.parallelize(List[PRMessage]()), numSplits) {
      (self: PRVertex, messages: Iterable[PRMessage], superstep: Int) =>
        val newValue =
          if (messages.nonEmpty)
            0.15 / numVertices + 0.85 * messages.map(_.value).sum
          else
            self.value

        val terminate = (superstep >= 10 && (newValue - self.value).abs < epsilon) || superstep >= 30

        val outbox =
          if (!terminate)
            self.outEdges.map(edge =>
              new PRMessage(edge.targetId, newValue / self.outEdges.size))
          else
            List()

        val newState = if (!terminate) Active else Inactive

        (new PRVertex(self.id, newValue, self.outEdges, newState), outbox)
    }

    // Print the result
    System.err.println("Articles with PageRank >= "+threshold+":")
    val top = result.filter(_.value >= threshold).map(vertex =>
      "%s\t%s\n".format(vertex.id, vertex.value)).collect.mkString
    println(top)
  }
}

@serializable class PRVertex(val id: String, val value: Double, val outEdges: Seq[PREdge], val state: VertexState) extends Vertex
@serializable class PRMessage(val targetId: String, val value: Double) extends Message
@serializable class PREdge(val targetId: String) extends Edge
