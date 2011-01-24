import spark._
import spark.SparkContext._

import scala.xml.{XML,NodeSeq}

object WikipediaPageRank {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: PageRank <inputFile> <threshold> <host>")
      System.exit(-1)
    }

    val inputFile = args(0)
    val threshold = args(1).toDouble
    val host = args(2)
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
        val outEdges = links.map(link =>
          Edge[Option[Nothing]](link.text, None))
        Vertex(title, 1.0 / numVertices, outEdges, Active)
      })

    // Do the computation
    val result = new Pregel().run(vertices, sc) {
      (self: Vertex[Double, Option[Nothing]], messages: Iterable[Message[Double]], superstep: Int) =>
        val newValue =
          if (messages.nonEmpty)
            0.15 / numVertices + 0.85 * messages.map(_.value).sum
          else
            self.value

        val outbox =
          if (superstep < 30)
            self.outEdges.map(edge =>
              Message(edge.targetId, newValue / messages.size))
          else
            List()

        val newState = if (superstep < 30) Inactive else Active

        (Vertex(self.id, newValue, self.outEdges, newState), outbox)
    }

    // Print the result
    System.err.println("Articles with PageRank >= "+threshold+":")
    val top = result.filter(_.value >= threshold).map(vertex =>
      "%s\t%s\n".format(vertex.id, vertex.value)).collect.mkString
    println(top)
  }
}