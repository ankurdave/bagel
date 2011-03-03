package bagel

import spark._
import spark.SparkContext._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

object Pregel {
  implicit def RDDExtensions[T](self: RDD[T]) = new RDDExtensions(self)
  implicit def PairRDDExtensions[K, V](self: RDD[(K, V)]) = new PairRDDExtensions(self)

  /**
   * Runs a Pregel job on the given vertices, running the specified
   * compute function on each vertex in every superstep. Before
   * beginning the first superstep, sends the given messages to their
   * destination vertices. In the join stage, launches splits
   * separate tasks (where splits is manually specified to work
   * around a bug in Spark).
   *
   * Halts when no more messages are being sent between vertices, and
   * all vertices have voted to halt by setting their state to
   * Inactive.
   */
  def run[V <: Vertex : Manifest, M <: Message : Manifest, C](vertices: RDD[V], messages: RDD[M], splits: Int, messageCombiner: (C, M) => C, defaultCombined: () => C, mergeCombined: (C, C) => C, superstep: Int = 0)(compute: (V, C, Int) => (V, Iterable[M])): RDD[V] = {
    println("Starting superstep "+superstep+".")
    val startTime = System.currentTimeMillis

    // Bring together vertices and messages
    println("Joining vertices and messages...")
    val verticesWithId = vertices.map(v => (v.id, v))
    val messagesWithId = messages.map(m => (m.targetId, m))
    val joined = verticesWithId.groupByKeyAsymmetrical(messagesWithId, messageCombiner, defaultCombined, mergeCombined, splits)
    println("Done joining vertices and messages.")

    // Run compute on each vertex
    println("Running compute on each vertex...")
    val processed = joined.flatMap {
      case (id, (None, ms)) => List()
      case (id, (Some(v), ms)) =>
          List(compute(v, ms, superstep))
    }.cache
    println("Done running compute on each vertex.")

    // Separate vertices from the messages they emitted
    println("Splitting vertices and messages...")
    val newVertices = processed.map(_._1)
    val newMessages = processed.map(_._2).flatMap(identity)
    println("Done splitting vertices and messages.")

    val timeTaken = System.currentTimeMillis - startTime
    println("Superstep %d took %d s".format(superstep, timeTaken / 1000))

    println("Checking stopping condition...")
    if (newMessages.count == 0 && newVertices.forall(_.state == Inactive))
      newVertices
    else
      run(newVertices, newMessages, splits, messageCombiner, defaultCombined, mergeCombined, superstep + 1)(compute)
  }
}

/**
 * Represents a Pregel vertex. Must be subclassed to store state
 * along with each vertex. Must be annotated with @serializable.
 */
trait Vertex {
  val id: String
  val state: VertexState
}

/**
 * Represents a Pregel message to a target vertex. Must be
 * subclassed to contain a payload. Must be annotated with @serializable.
 */
trait Message {
  val targetId: String
}

/**
 * Represents a directed edge between two vertices. Owned by the
 * source vertex, and contains the ID of the target vertex. Must
 * be subclassed to store state along with each edge. Must be annotated with @serializable.
 */
trait Edge {
  val targetId: String
}

/**
 * Case enumeration representing the state of a Pregel vertex. Active
 * vertices run their computation in every superstep. Inactive
 * vertices have voted to halt and do not run computation unless they
 * receive a message.
 */
sealed abstract class VertexState
case object Active extends VertexState
case object Inactive extends VertexState
