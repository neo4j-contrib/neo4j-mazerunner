package org.mazerunner.core.programs

import org.apache.spark.graphx._
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.mazerunner.core.config.ConfigurationLoader
import org.mazerunner.core.processor.GraphProcessor
import org.scalatest.FlatSpec

/**
 * Copyright (C) 2014 Kenny Bastani
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
class ShortestPathProgramTests extends FlatSpec {
  /**
   * To collect the shortest path results for all nodes to a single destination node,
   * the following steps must be taken:
   *
   */

  ConfigurationLoader.testPropertyAccess = true

  // Create Spark context
  val sc = GraphProcessor.initializeSparkContext.sc



  def fixture =
    new {
      // Create an RDD for the vertices
      val vertices: RDD[(VertexId, ShortestPathState)] = sc.parallelize(Array(
        (0L, new ShortestPathState(0L, 3L)),
        (1L, new ShortestPathState(1L, 3L)),
        (2L, new ShortestPathState(2L, 3L)),
        (3L, new ShortestPathState(3L, 3L)),
        (4L, new ShortestPathState(4L, 3L)),
        (5L, new ShortestPathState(5L, 3L)),
        (6L, new ShortestPathState(6L, 3L)),
        (7L, new ShortestPathState(7L, 3L)),
        (8L, new ShortestPathState(8L, 3L)),
        (9L, new ShortestPathState(9L, 3L)),
        (10L, new ShortestPathState(10L, 3L)),
        (11L, new ShortestPathState(11L, 3L)),
        (12L, new ShortestPathState(12L, 3L))))

      // Create an RDD for edges
      val edges: RDD[Edge[Int]] = sc.parallelize(Array(
        Edge(0L, 1L, 0),
        Edge(1L, 4L, 0),
        Edge(1L, 2L, 0),
        Edge(2L, 3L, 0),
        Edge(5L, 6L, 0),
        Edge(6L, 7L, 0),
        Edge(7L, 8L, 0),
        Edge(8L, 9L, 0),
        Edge(9L, 10L, 0),
        Edge(10L, 11L, 0),
        Edge(11L, 12L, 0),
        Edge(12L, 3L, 0),
        Edge(7L, 3L, 0),
        Edge(4L, 3L, 0)))

      // Build the initial Graph
      val graph = Graph(vertices, edges, new ShortestPathState(-1L, -1L))
    }

  /**
   * 0. The state of each node is represented as a decision tree
   * a. Each node will need to manage a serializable graph data structure
   * b. Each node can have a super state of active or inactive
   * c. Active nodes have at least one non-dead branch without the destination node in it
   */

  "A node's state" should "have a decision tree" in {
    val graph = fixture.graph

    // Get a vertex neighbor map
    val vertexNeighborMap = new PairRDDFunctions[VertexId, Array[(VertexId, ShortestPathState)]](graph.ops.collectNeighbors(EdgeDirection.Out)
      .map(a => (a._1, a._2) : (VertexId, Array[(VertexId, ShortestPathState)])))
    .collectAsMap()

    // Get all paths to destination node as a decision tree
    val results = graph.triplets.map(triplet => {
      triplet.srcAttr.decisionTree.addLeaf(triplet.dstId)
      def addLeaf(active : VertexId, subActive : VertexId) : Unit = new {
        if(subActive != triplet.srcAttr.dstVertex) {
          vertexNeighborMap(subActive).foreach(f => {
            if(triplet.srcAttr.addToPath(f._1, subActive))
              addLeaf(active, f._1)
          })
        }
      }
      addLeaf(triplet.srcId, triplet.dstId)
      triplet.srcAttr
    }).map(a => (a.srcVertex, a))

    // Use collectAsMap to bring everything on the same thread
    new PairRDDFunctions[VertexId, ShortestPathState](results).collectAsMap()
      .foreach(p => println(p._2.decisionTree.toString))
  }



  /**
   * c.
   * 1. The node's state must maintain memory of all shortest paths for each super step iteration
   * a. Shortest paths are represented as branches of a decision tree for the node's state
   * 2. The node's state must maintain a list of sub-active nodes that will be signaled at the start of the next iteration
   * a. Sub-active nodes are the nodes on the end of each branch of the decision tree for a node's state
   * 3. Each sub-active node must not be the destination node
   * 4. Each sub-active node will receive a message from an active node and send a message back to the active node with a list of its outgoing edges and its current state
   * 5. Each sub-active node will provide information back to the active node about its active state for the last iteration
   * 6. A node can be dead, mortal, or immortal
   * a. Dead nodes are inactive
   * b. Immortal nodes cannot be active but can be sub-active
   * c. Mortal nodes can be both active and sub-active
   * 7. Dead nodes have no path to the destination node
   * 8. Dead nodes will return back no outgoing edges to the source node, ending traversal for that branch
   * 9. A node can become dead only if it is mortal
   * 10. A node becomes dead when it has no outgoing edges and it is not the source/destination node
   * 11. A node becomes dead when all branches of its decision tree have at least one dead node in it
   * 12. When a mortal node receives a message back from a dead node, it updates its decision tree, killing branches with dead nodes
   * 13. A mortal node is a node that has non-dead branches in its decision tree
   * 14. A mortal node is a node that has no destination node in any branch of its decision tree
   * 15. A mortal node becomes immortal when one of its branches ends at the destination node
   * 16. Immortal nodes cannot become dead or mortal
   * 17. When a sub-active node signals back to an active node, it will provide its status ([active, inactive], [dead, mortal, immortal])
   */

  "A Stack" should "pop values in last-in-first-out order" in {
//    val tree : DecisionTree[VertexId] = new DecisionTree[VertexId](0L)
//    tree.traverseTo(0L).addLeaf(1L).addLeaf(2L)
//    tree.traverseTo(1L).addLeaf(3L).addLeaf(4L)
//    tree.traverseTo(3L).addLeaf(5L)
//
//    println(tree)
  }

}
