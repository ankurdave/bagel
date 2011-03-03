package bagel

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

    println("Counting vertices...")
    val numVertices = input.count()
    println("Done counting vertices.")

    println("Parsing input file...")
    val vertices = input.map(line => {
      val fields = line.split("\t")
      val (title, body) = (fields(1), fields(3).replace("\\n", "\n"))
      val links =
        if (body == "\\N")
          NodeSeq.Empty
        else
          try {
            XML.loadString(body) \\ "link" \ "target"
          } catch {
            case e: org.xml.sax.SAXParseException =>
              System.err.println("Article \""+title+"\" has malformed XML in body:\n"+body)
            NodeSeq.Empty
          }
      val outEdges = links.map(link => new PREdge(new String(link.text))).toArray
      new PRVertex(new String(title), 1.0 / numVertices, outEdges, Active)
    }).cache

    println("Done parsing input file.")
    println("Input file had "+vertices.count() + " vertices.")

    // Do the computation
    val epsilon = 0.01 / numVertices

    def messageCombiner(minSoFar: Double, message: PRMessage): Double =
      minSoFar + message.value
    def mergeCombined(a: Double, b: Double) = a + b

    val result = Pregel.run(vertices, sc.parallelize(List[PRMessage]()), numSplits, messageCombiner, 0.0, mergeCombined) {
      (self: PRVertex, messageSum: Double, superstep: Int) =>
        val newValue =
          if (messageSum != 0)
            0.15 / numVertices + 0.85 * messageSum
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