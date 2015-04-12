package org.mazerunner.core.programs

import org.apache.spark.graphx.{Graph, EdgeTriplet, VertexId}
import org.mazerunner.core.abstractions.PregelProgram

/**
 * The [[BetweennessCentralityProgram]] calculates the closeness centrality of each
 * vertex in the graph using the [[PregelProgram]] abstraction
 */
class BetweennessCentralityProgram(@transient val graph : Graph[ShortestPathState, Double], val source : Seq[VertexId])
  extends PregelProgram[ShortestPathState, ShortestPathState, Double] with Serializable {

  /**
   * For serialization purposes.
   * See: [[org.apache.spark.graphx.impl.GraphImpl]]
   */
  protected def this() = this(null, Seq[VertexId]())

  /**
   *
   * @param id is the [[VertexId]] that this program will perform a state operation for
   * @param state is the current state of this [[VertexId]]
   * @param message is the state received from another vertex in the graph
   * @return a [[ShortestPathState]] resulting from a comparison between current state and incoming state
   */
  override def vertexProgram(id: VertexId, state: ShortestPathState, message: ShortestPathState): ShortestPathState = {
    if (state.decisionTree.root != id) {
      val result: ShortestPathState = new ShortestPathState(id, source)
      return result
    }

    if (message.decisionTree.traverseTo(id) != null) {
      def copyTree(branch: DecisionTree[VertexId], item: VertexId, sequence: Seq[VertexId]) {
        branch.branches.foreach(a => {
          state.decisionTree.traverseTo(item).addLeaf(a.root)
          if(!sequence.contains(a.root))
            copyTree(a, a.root, sequence ++: Seq[VertexId](a.root))
        })
      }
      copyTree(message.decisionTree.traverseTo(id), id, Seq[VertexId](id))
    }

    for (x <- message.neighborsIn if x == id) {
      if (!state.neighborsOut.contains(message.srcVertex)) {
        state.neighborsOut = state.neighborsOut ++ Seq[VertexId](message.srcVertex)
      }
    }

    for (x <- message.neighborsOut if x == id) {
      if (!state.neighborsIn.contains(message.srcVertex)) {
        state.neighborsIn = state.neighborsIn ++ Seq[VertexId](message.srcVertex)
      }
    }

    state.superState = message.superState

    state.stage = state.stage + 1

    state
  }

  /**
   * Return the larger of the two vertex state results
   * @param state A first [[Int]] representing a partial state of a vertex.
   * @param message A second [[Int]] representing a different partial state of a vertex
   * @return a merged [[Int]] representation from the two [[Int]] parameters
   */
  override def combiner(state: ShortestPathState, message: ShortestPathState): ShortestPathState = {

    if (state.decisionTree.traverseTo(message.srcVertex) != null) {
      def copyTree(branch: DecisionTree[VertexId], item: VertexId, sequence: Seq[VertexId]) {
        branch.branches.foreach(a => {
          state.decisionTree.traverseTo(item).addLeaf(a.root)
          if(!sequence.contains(a.root))
            copyTree(a, a.root, sequence ++: Seq[VertexId](a.root))
        })
      }
      copyTree(message.decisionTree.traverseTo(message.srcVertex), message.srcVertex, Seq[VertexId](message.srcVertex))
    }

    state
  }

  /**
   * If the dstVertex's value is less than the srcVertex's value, send a message to the dstVertex to update
   * its state
   * @param triplet An edge triplet is an object containing a pair of connected vertex objects and edge object.
   *                For example (v1)-[r]->(v2)
   * @return The message broker returns a key value list, each containing a VertexId and a new message
   */
  override def messageBroker(triplet: EdgeTriplet[ShortestPathState, Double]): Iterator[(VertexId, ShortestPathState)] = {
    // This must be run for each vertex in the graph
    if (triplet.srcAttr == null) {
      return {
        val results = for (vertexId <- source) yield {
          val message: ShortestPathState = new ShortestPathState(triplet.srcId, source)
          message.addToPath(triplet.srcId, triplet.dstId)
          (vertexId, message)
        }
        results.toIterator
      }
    }

    if (triplet.dstAttr == null) {
      return {
        val results = for (vertexId <- source) yield {
          val message: ShortestPathState = new ShortestPathState(triplet.dstId, source)
          message.addToPath(triplet.dstId, triplet.srcId)
          (vertexId, message)
        }
        results.toIterator
      }
    }


    // Collect all neighbors to determine when pregel finishes
    if (!triplet.dstAttr.neighborsIn.contains(triplet.srcId)) {
      triplet.dstAttr.neighborsIn = triplet.dstAttr.neighborsIn ++ Seq[VertexId](triplet.srcId)
    }

    if (!triplet.srcAttr.neighborsOut.contains(triplet.dstId)) {
      triplet.srcAttr.neighborsOut = triplet.srcAttr.neighborsOut ++ Seq[VertexId](triplet.dstId)
    }

    {
      if (triplet.srcAttr.decisionTree.traverseTo(triplet.srcId) != null) {
        if (triplet.srcAttr.addToPath(triplet.dstId, triplet.srcId)) {
          Seq[(VertexId, ShortestPathState)]((triplet.srcAttr.srcVertex, triplet.srcAttr), (triplet.srcAttr.srcVertex, triplet.dstAttr))
        } else {
          if (triplet.dstAttr.neighborsOut.exists(a => triplet.srcAttr.addToPath(a, triplet.dstId))) {
            Seq[(VertexId, ShortestPathState)]((triplet.srcAttr.srcVertex, triplet.srcAttr), (triplet.dstAttr.srcVertex, triplet.dstAttr))
          } else {
            val keys = triplet.srcAttr.decisionTree.graph.keys.size
            if(combiner(triplet.srcAttr, triplet.dstAttr).decisionTree.graph.keys.size > keys) {
              Seq[(VertexId, ShortestPathState)]((triplet.srcAttr.srcVertex, combiner(triplet.srcAttr, triplet.dstAttr)))
            } else {
              Seq[(VertexId, ShortestPathState)]()
            }

          }
        }
      } else {
        Seq[(VertexId, ShortestPathState)]()
      }
    }.toIterator
  }


  /**
   * This method wraps Spark's Pregel API entry point from the [[org.apache.spark.graphx.GraphOps]] class.
   * This provides a simple way to write a suite of graph algorithms by extending the [[PregelProgram]]
   * abstract class and implementing vertexProgram, messageBroker, and combiner methods.
   */
  def run(): Graph[ShortestPathState, Double] = {
    graph.pregel(new ShortestPathState(-1L, source))(this.vertexProgram, this.messageBroker, this.combiner)
  }

}