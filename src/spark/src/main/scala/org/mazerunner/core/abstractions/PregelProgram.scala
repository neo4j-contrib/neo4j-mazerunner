package org.mazerunner.core.abstractions

import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
 * The [[PregelProgram]] abstraction wraps Spark's Pregel API implementation from the [[GraphOps]]
 * class into a model that is easier to write graph algorithms.
 * @tparam VertexState is the generic type representing the state of a vertex
 */
abstract class PregelProgram[VertexState: ClassTag, VD: ClassTag, ED: ClassTag] protected () extends Serializable {

  @transient val graph: Graph[VD, ED]

  /**
   * The vertex program receives a state update and acts to update its state
   * @param id is the [[VertexId]] that this program will perform a state operation for
   * @param state is the current state of this [[VertexId]]
   * @param message is the state received from another vertex in the graph
   * @return a [[VertexState]] resulting from a comparison between current state and incoming state
   */
  def vertexProgram(id : VertexId, state : VertexState, message : VertexState) : VertexState

  /**
   * The message broker sends and receives messages. It will initially receive one message for
   * each vertex in the graph.
   * @param triplet An edge triplet is an object containing a pair of connected vertex objects and edge object.
   *                For example (v1)-[r]->(v2)
   * @return The message broker returns a key value list, each containing a VertexId and a new message
   */
  def messageBroker(triplet :EdgeTriplet[VertexState, ED]) : Iterator[(VertexId, VertexState)]

  /**
   * This method is used to reduce or combine the set of all state outcomes produced by a vertexProgram
   * for each vertex in each superstep iteration. Each vertex has a list of state updates received from
   * other vertices in the graph via the messageBroker method. This method is used to reduce the list
   * of state updates into a single state for the next superstep iteration.
   * @param a A first [[VertexState]] representing a partial state of a vertex.
   * @param b A second [[VertexState]] representing a different partial state of a vertex
   * @return a merged [[VertexState]] representation from the two [[VertexState]] parameters
   */
  def combiner(a: VertexState, b: VertexState) : VertexState

}