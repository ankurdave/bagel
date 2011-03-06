package bagel

import spark._
import spark.SparkContext._

import scala.collection.mutable.ArrayBuffer

import scala.xml.{XML,NodeSeq}

import java.io.{Externalizable,ObjectInput,ObjectOutput,DataOutputStream,DataInputStream}

object WikipediaPageRank {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: PageRank <inputFile> <threshold> <numSplits> <host> [<noCombiner>]")
      System.exit(-1)
    }

    val inputFile = args(0)
    val threshold = args(1).toDouble
    val numSplits = args(2).toInt
    val host = args(3)
    val noCombiner = args.length > 4 && args(4).nonEmpty
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
      val outEdges = ArrayBuffer(links.map(link => new PREdge(new String(link.text))): _*)
      new PRVertex(new String(title), 1.0 / numVertices, outEdges, Active)
    }).cache

    println("Done parsing input file.")
    println("Input file had "+vertices.count() + " vertices.")

    // Do the computation
    val epsilon = 0.01 / numVertices
    val result =
      if (noCombiner)
        Pregel.run(vertices, sc.parallelize(List[PRMessage]()), numSplits, NoCombiner.messageCombiner, NoCombiner.defaultCombined, NoCombiner.mergeCombined)(NoCombiner.compute(numVertices, epsilon))
      else
        Pregel.run(vertices, sc.parallelize(List[PRMessage]()), numSplits, Combiner.messageCombiner, Combiner.defaultCombined, Combiner.mergeCombined)(Combiner.compute(numVertices, epsilon))

    // Print the result
    System.err.println("Articles with PageRank >= "+threshold+":")
    val top = result.filter(_.value >= threshold).map(vertex =>
      "%s\t%s\n".format(vertex.id, vertex.value)).collect.mkString
    println(top)
  }

  object Combiner {
    def messageCombiner(minSoFar: Double, message: PRMessage): Double =
      minSoFar + message.value

    def mergeCombined(a: Double, b: Double) = a + b

    def defaultCombined(): Double = 0.0

    def compute(numVertices: Long, epsilon: Double)(self: PRVertex, messageSum: Double, superstep: Int): (PRVertex, Iterable[PRMessage]) = {
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
          ArrayBuffer[PRMessage]()

      val newState = if (!terminate) Active else Inactive

      (new PRVertex(self.id, newValue, self.outEdges, newState), outbox)
    }
  }

  object NoCombiner {
    def messageCombiner(messagesSoFar: ArrayBuffer[PRMessage], message: PRMessage): ArrayBuffer[PRMessage] =
      messagesSoFar += message

    def mergeCombined(a: ArrayBuffer[PRMessage], b: ArrayBuffer[PRMessage]): ArrayBuffer[PRMessage] =
      a ++= b

    def defaultCombined(): ArrayBuffer[PRMessage] = ArrayBuffer[PRMessage]()

    def compute(numVertices: Long, epsilon: Double)(self: PRVertex, messages: Seq[PRMessage], superstep: Int): (PRVertex, Iterable[PRMessage]) =
      Combiner.compute(numVertices, epsilon)(self, messages.map(_.value).sum, superstep)
  }
}

@serializable class PRVertex(var id: String, var value: Double, var outEdges: ArrayBuffer[PREdge], var state: VertexState) extends Vertex with Externalizable {
  def writeExternal(out: ObjectOutput) {
    val idBytes = id.getBytes()
    out.writeInt(idBytes.length)
    out.write(idBytes)
    out.writeDouble(value)
    out.writeInt(outEdges.length)
    for (e <- outEdges) {
      val eBytes = e.targetId.getBytes()
      out.writeInt(eBytes.length)
      out.write(eBytes)
    }
    out.writeBoolean(state match {
      case Active => true
      case Inactive => false
    })
  }

  def readExternal(in: ObjectInput) {
    val idLength = in.readInt()
    val idBytes = new Array[Byte](idLength)
    in.read(idBytes, 0, idLength)
    id = new String(idBytes)
    value = in.readDouble()
    outEdges = new ArrayBuffer[PREdge]
    for (i <- 0 until in.readInt()) {
      val eLength = in.readInt()
      val eBytes = new Array[Byte](eLength)
      in.read(eBytes, 0, eLength)
      outEdges += new PREdge(new String(eBytes))
    }
    state = if (in.readBoolean()) Active else Inactive
  }
}

@serializable class PRMessage(var targetId: String, var value: Double) extends Message with Externalizable {
  def writeExternal(out: ObjectOutput) {
    val idBytes = targetId.getBytes()
    out.writeInt(idBytes.length)
    out.write(idBytes)
    out.writeDouble(value)
  }

  def readExternal(in: ObjectInput) {
    val idLength = in.readInt()
    val idBytes = new Array[Byte](idLength)
    in.read(idBytes, 0, idLength)
    targetId = new String(idBytes)
    value = in.readDouble()
  }
}

@serializable class PREdge(var targetId: String) extends Edge with Externalizable {
  def writeExternal(out: ObjectOutput) {
    val idBytes = targetId.getBytes()
    out.writeInt(idBytes.length)
    out.write(idBytes)
  }

  def readExternal(in: ObjectInput) {
    val idLength = in.readInt()
    val idBytes = new Array[Byte](idLength)
    in.read(idBytes, 0, idLength)
    targetId = new String(idBytes)
  }
}
