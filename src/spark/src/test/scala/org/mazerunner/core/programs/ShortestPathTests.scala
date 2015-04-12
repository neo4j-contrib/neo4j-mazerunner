package org.mazerunner.core.programs

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
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
class ShortestPathTests  extends FlatSpec {
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

  "A node's state" should "have a decision tree" in {
    val graph = fixture.graph

    val tree  = new DecisionTree[VertexId](0L)
    val tree2 = new DecisionTree[VertexId](0L)
//    tree.traverseTo(0L).addLeaf(1L)
//    tree.traverseTo(0L).addLeaf(2L).addLeaf(3L).addLeaf(4L).addLeaf(5L)
//    tree.traverseTo(4L).addLeaf(6L).addLeaf(9L)
//    tree.traverseTo(4L).addLeaf(7L).addLeaf(8L).addLeaf(9L)
//    tree.traverseTo(7L).addLeaf(9L)

    tree.traverseTo(0L).addLeaf(1L).addLeaf(4L)
    tree.traverseTo(0L).addLeaf(4L).addLeaf(3L)
    tree.traverseTo(1L).addLeaf(2L).addLeaf(3L)

    System.out.println(tree.toString())

    System.out.println(tree.allShortestPathsTo(0L))
    System.out.println(tree.allShortestPathsTo(1L))
    System.out.println(tree.allShortestPathsTo(2L))
    System.out.println(tree.allShortestPathsTo(3L))
    System.out.println(tree.allShortestPathsTo(4L))



    //val nodeList: List[String] = Arrays.asList("0 1\n", "0 4\n", "4 3\n", "1 4\n", "1 2\n", "2 3")

//    // Get the shortest path and add it to the decision tree
//    val shortestPath = tree.shortestPathTo(9L)
//
//    val shortestPathArray = shortestPath.toArray
//
//    for (x <- shortestPathArray; y = shortestPathArray.indexOf(x)) {
//      if (y < shortestPath.length - 1)
//        tree2.traverseTo(x).addLeaf(shortestPathArray(y + 1))
//    }

    System.out.println(tree2)
  }

}
