package org.mazerunner.core.programs

import org.apache.spark.graphx.{Graph, EdgeTriplet, VertexId}
import org.mazerunner.core.abstractions.PregelProgram

/**
 * The [[MaximumValueProgram]] is an example graph algorithm implemented on the [[PregelProgram]]
 * abstraction.
 */
class MaximumValueProgram(@transient val graph : Graph[Int, Int])
  extends PregelProgram[Int, Int, Int] with Serializable {

  protected def this() = this(null)

  /**
   * Return the larger of the two vertex attribute values
   * @param id is the [[VertexId]] that this program will perform a state operation for
   * @param state is the current state of this [[VertexId]]
   * @param message is the state received from another vertex in the graph
   * @return an [[Int]] resulting from a comparison between current state and incoming state
   */
  override def vertexProgram(id: VertexId, state: Int, message: Int): Int = {
    if (message > state) {
      message
    } else {
      state
    }
  }

  /**
   * Return the larger of the two vertex state results
   * @param a A first [[Int]] representing a partial state of a vertex.
   * @param b A second [[Int]] representing a different partial state of a vertex
   * @return a merged [[Int]] representation from the two [[Int]] parameters
   */
  override def combiner(a: Int, b: Int): Int = {
    math.max(a, b)
  }

  /**
   * If the dstVertex's value is less than the srcVertex's value, send a message to the dstVertex to update
   * its state
   * @param triplet An edge triplet is an object containing a pair of connected vertex objects and edge object.
   *                For example (v1)-[r]->(v2)
   * @return The message broker returns a key value list, each containing a VertexId and a new message
   */
  override def messageBroker(triplet: EdgeTriplet[Int, Int]): Iterator[(VertexId, Int)] = {
    // If the srcAttr is greater than the dstAttr then notify the dstVertex to update its state
    if (triplet.srcAttr > triplet.dstAttr) {
      Iterator((triplet.dstId, triplet.srcAttr))
    } else {
      Iterator.empty
    }
  }

  /**
   * This method wraps Spark's Pregel API entry point from the [[org.apache.spark.graphx.GraphOps]] class. This provides
   * a simple way to write a suite of graph algorithms by extending the [[PregelProgram]] abstract
   * class and implementing vertexProgram, messageBroker, and combiner methods.
   * @param initialMsg is the initial message received for all vertices in the graph
   */
  def run(initialMsg: Int): Graph[Int, Int] = {
    graph.pregel(initialMsg)(this.vertexProgram, this.messageBroker, this.combiner)
  }
}
