package org.mazerunner.core.programs

import com.github.mdr.ascii.layout.GraphLayout
import com.github.mdr.ascii.layout.prefs.LayoutPrefsImpl
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}
import org.mazerunner.core.abstractions.PregelProgram
import org.mazerunner.core.programs.SuperState.SuperState

import scala.collection.mutable

/**
 * @author Kenny Bastani
 * The [[ShortestPathProgram]] calculates the single source shortest path for a graph
 */
class ShortestPathProgram(@transient val graph : Graph[ShortestPathState, Double], val source : VertexId)
  extends PregelProgram[ShortestPathState, ShortestPathState, Double] with Serializable {

  /**
   * For serialization purposes.
   * See: [[org.apache.spark.graphx.impl.GraphImpl]]
   */
  protected def this() = this(null, 0)

  /**
   *
   * @param id is the [[VertexId]] that this program will perform a state operation for
   * @param state is the current state of this [[VertexId]]
   * @param message is the state received from another vertex in the graph
   * @return an [[Int]] resulting from a comparison between current state and incoming state
   */
  override def vertexProgram(id: VertexId, state: ShortestPathState, message: ShortestPathState): ShortestPathState = {
    //println("State for " + id + " -> " + state.srcVertex + " " + state.dstVertex + ": " + state.decisionTree + "\nMessage for "  + id + " -> " + state.srcVertex + " " + state.dstVertex + ": " + message.decisionTree)


    if(state.decisionTree.root != id) {
      val result: ShortestPathState = new ShortestPathState(id, source)
      return result
    }

    if(message.decisionTree.traverseTo(id) != null) {
      message.decisionTree.traverseTo(id).branches.foreach(b => if(state.decisionTree.traverseTo(b.root) == null) {
        state.decisionTree.branches.add(b)
      })
    }

    state
  }

  /**
   * Return the larger of the two vertex state results
   * @param state A first [[Int]] representing a partial state of a vertex.
   * @param message A second [[Int]] representing a different partial state of a vertex
   * @return a merged [[Int]] representation from the two [[Int]] parameters
   */
  override def combiner(state: ShortestPathState, message: ShortestPathState): ShortestPathState = {
    if(state.decisionTree.traverseTo(message.srcVertex) != null) {
      message.decisionTree.traverseTo(message.srcVertex).branches.foreach(b => if(state.decisionTree.traverseTo(b.root) == null) {
        state.decisionTree.traverseTo(message.srcVertex).branches.add(b)
      })
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

    // Every edge in the graph will be activated once every super stage

    if (triplet.srcAttr.srcVertex == -1L) {
      val message: ShortestPathState = new ShortestPathState(triplet.srcId, source)
      message.addToPath(triplet.dstId, triplet.srcId)
      if(triplet.dstAttr.srcVertex != -1L) {
        return Iterator.empty
      } else {
        return Iterator((triplet.srcId, message))
      }
    }

    if(triplet.srcId != triplet.srcAttr.dstVertex) {
        if(triplet.dstAttr != null && triplet.srcAttr.srcVertex != -1L) {
          // Check if the dstVertex is inactive
          if(triplet.dstAttr.superState != SuperState.Inactive) {
            // Check if the source vertex has visited this vertex yet
            if(triplet.srcAttr.decisionTree.traverseTo(triplet.dstId) == null) {
              // Check if dstVertex has any more moves
              // Add this vertex to srcVertex's decision tree
              if(triplet.srcAttr.addToPath(triplet.dstId, triplet.srcId)) {
                return Iterator((triplet.srcId, triplet.srcAttr), (triplet.dstId, triplet.srcAttr))
              } else {
                return Iterator.empty
              }
            } else {
              // Check if dstVertex has found a path to the destination
              val dstCheck = triplet.dstAttr.decisionTree.traverseTo(triplet.dstAttr.dstVertex)
              if(dstCheck != null) {
                // Get the shortest path and replicate it to this vertex's decision tree
                val shortestPath = triplet.dstAttr.decisionTree.shortestPathTo(triplet.dstAttr.dstVertex)
                val shortestPathArray = shortestPath.toArray
                for (x <- shortestPathArray; y = shortestPathArray.indexOf(x)) {
                  if(y < shortestPath.length - 1)
                    triplet.srcAttr.decisionTree.traverseTo(x).addLeaf(shortestPathArray(y + 1))
                }
              } else {
                // Traverse to the next path
                return Iterator.empty
              }
            }
          }
        }
      }


    Iterator()
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

/**
 * The vertex state object for finding all shortest paths
 * @param srcVertex is the source vertex to traverse from
 * @param dstVertex is the destination vertex to traverse to
 */
class ShortestPathState(val srcVertex: VertexId, val dstVertex: VertexId) extends Serializable {

  /**
   * The [[SuperState]] is used to track whether or not this vertex has an optimized path to the destination
   */
  var superState : SuperState = SuperState.Active

  /**
   * A [[DecisionTree]] is used to provide a stateful mechanism for finding the shortest path to the destination vertex
   */
  val decisionTree = new DecisionTree[VertexId](srcVertex)

  def getShortestPaths : Seq[Seq[VertexId]] = {
    // Check if there is a path to the destination
    decisionTree.allShortestPathsTo(dstVertex)
  }

  /**
   * Adds a new branch to the decision tree for this vertex representing the paths it has already explored
   * @param to is the new leaf to add to the branch
   * @param from is the branch to add the new leaf on
   * @return a [[Boolean]] representing whether or not the operation was successful
   */
  def addToPath(to: VertexId, from: VertexId) : Boolean = {

    if(from != dstVertex) {
      val endNode = decisionTree.traverseTo(from)
      if(endNode != null) {
        if(endNode.traverseTo(to) == null) {
          return endNode.addLeaf(to) != null
        } else false
      } else false
    }

    val thisEndNode = decisionTree.traverseTo(from)

    if(thisEndNode != null) {
      if(thisEndNode.traverseTo(to) == null) {
        return thisEndNode.addLeaf(to) != null
      } else {
        false
      }
    } else false

    //to != dstVertex
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[ShortestPathState]

  override def equals(other: Any): Boolean = other match {
    case that: ShortestPathState =>
      (that canEqual this) &&
        decisionTree == that.decisionTree &&
        srcVertex == that.srcVertex &&
        dstVertex == that.dstVertex
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(decisionTree, srcVertex, dstVertex)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

/**
 * The SuperState of a [[ShortestPathState]]
 */
object SuperState extends Enumeration with Serializable {
  type SuperState = Value
  val Active, Inactive = Value
}

/**
 * The [[DecisionTree]] provides a mechanism for planning the optimal shortest path from a source vertex to a destination vertex
 * @param root is the id of the vertex that is at the base of this [[DecisionTree]]
 * @tparam VD is the vertex id type for the decision tree
 */
class DecisionTree[VD](val root : VD) extends Serializable {

  /**
   * The branches for this decision tree, consisting of a set of leaf vertices that have context to a traversable branch
   */
  var branches : scala.collection.mutable.SynchronizedSet[DecisionTree[VD]] = new scala.collection.mutable.HashSet[DecisionTree[VD]]() with mutable.SynchronizedSet[DecisionTree[VD]]

  /**
   * Adds a leaf vertex to this branch
   * @param leaf is the vertex id of the new leaf
   * @return a new branch for this leaf
   */
  def addLeaf(leaf : VD) : DecisionTree[VD] = {
    val newLeaf : DecisionTree[VD] = { new DecisionTree[VD](leaf) }
    this.branches.add(newLeaf)
    newLeaf
  }

  /**
   * Traverses the decision tree until it finds a branch that matches a supplied vertex id
   * @param item is the vertex id of the item to traverse to
   * @return a branch for the desired vertex
   */
  def traverseTo(item : VD) : DecisionTree[VD] = {
    if(item == root) {
      this
    } else {
      val result = {
        for (branch <- branches ; x = branch.traverseTo(item) if x != null) yield x
      }.take(1)
        .find(a => a != null)
        .getOrElse(null)

      result
    }
  }


  /**
   * Gets the shortest path to the item or else returns null
   * @param item is the id of the vertex to traverse to
   * @return the shortest path as a sequence of [[VD]]
   */
  def shortestPathTo(item : VD) : Seq[VD] = {
    if(item == root) {
      Seq[VD](this.root)
    } else {
      val result = {
        for (branch <- branches ; x = branch.shortestPathTo(item) if x != null) yield x
      }.toSeq.sortBy(b => b.length).take(1)
        .find(a => a != null)
        .getOrElse(null)

      result match {
        case x: Seq[VD] => x.+:(root)
        case x => null
      }
    }
  }

  def allShortestPathsTo(item : VD, distance : Int, depth : Int) : Seq[Seq[VD]] = {
    if(depth > distance) return null

    if(item == root) {
        Seq(Seq[VD](this.root))
    } else {
      val result = {
        for (branch <- branches ; x = branch.allShortestPathsTo(item, distance, depth + 1) if x != null) yield x
      }

      result match {
        case x: scala.collection.mutable.HashSet[Seq[Seq[VD]]] => {
          result.flatMap(a => a.toSeq).map(a => Seq[VD](this.root) ++ a.toSeq).toSeq
        }
        case x => null
      }
    }
  }

  def allShortestPathsTo(item : VD) : Seq[Seq[VD]] = {
    val shortestPath = shortestPathTo(item)

    if(shortestPath != null && shortestPath.length > 2) {
      val thisShortestPath = allShortestPathsTo(item, shortestPath.length, 1).map(a => a.tail.take(1))
      if(thisShortestPath.length > 0) {
        thisShortestPath
      } else {
        null
      }
    } else {
      null
    }
  }

  /**
   * Converts a [[DecisionTree]] to a [[com.github.mdr.ascii.graph.Graph]] which can be rendered in ASCII art
   * @return a [[com.github.mdr.ascii.graph.Graph]] that can be visualized as ASCII art
   */
  def toGraph: com.github.mdr.ascii.graph.Graph[String] = {
    val edges: scala.collection.mutable.Set[(String, String)] = scala.collection.mutable.Set[(String, String)]()
    val vertices: scala.collection.mutable.Set[String] = scala.collection.mutable.Set[String]()

    vertices.add(root.toString)
    branches.map(a => (root.toString, a.root.toString)).foreach(a => edges.add(a))

    branches.foreach(a => {
      val thisGraph = a.toGraph
      thisGraph.vertices.foreach(b => vertices.add(b))
      thisGraph.edges.foreach(b => edges.add(b))
    })

    com.github.mdr.ascii.graph.Graph(vertices.toList.toSet, edges.toList)
  }

  /**
   * Renders the [[DecisionTree]] in ASCII art
   * @return a [[String]] that has a graph layout visualization in ASCII art of the [[DecisionTree]]
   */
  override def toString: String = {
    val layoutPrefs = LayoutPrefsImpl(unicode = true,
      explicitAsciiBends = true,
      compactify = false,
      removeKinks = true,
      vertical = false,
      doubleVertices = false,
      rounded = false)

    "\n" + GraphLayout.renderGraph(this.toGraph, layoutPrefs = layoutPrefs)
  }
}

