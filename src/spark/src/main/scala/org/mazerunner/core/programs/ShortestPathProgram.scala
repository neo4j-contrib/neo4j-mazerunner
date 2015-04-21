package org.mazerunner.core.programs

import com.github.mdr.ascii.layout.GraphLayout
import com.github.mdr.ascii.layout.prefs.LayoutPrefsImpl
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}
import org.mazerunner.core.abstractions.PregelProgram
import org.mazerunner.core.programs.SuperState.SuperState

import scala.collection.mutable

/**
 * @author Kenny Bastani
 * The [[ShortestPathProgram]] calculates the single source shortest path for a graph
 */
class ShortestPathProgram(@transient val graph : Graph[ShortestPathState, Double], val source : Seq[VertexId])
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
   * @return an [[Int]] resulting from a comparison between current state and incoming state
   */
  override def vertexProgram(id: VertexId, state: ShortestPathState, message: ShortestPathState): ShortestPathState = {
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

    // This must be run for each vertex in the graph
    if(triplet.srcAttr == null) {
      return {
        val results = for (vertexId <- source) yield {
          val message: ShortestPathState = new ShortestPathState(triplet.srcId, source)
          message.addToPath(triplet.dstId, triplet.srcId)
          (vertexId, message)
        }
        results.toIterator
      }
    }

    if(triplet.dstAttr == null) {
      return {
        val results = for (vertexId <- source) yield {
          val message: ShortestPathState = new ShortestPathState(triplet.dstId, source)
          message.addToPath(triplet.dstId, triplet.srcId)
          (vertexId, message)
        }
        results.toIterator
      }
    }

    val results = for (vertexId <- source) yield {
        if (triplet.srcId != vertexId) {
          if (triplet.dstAttr != null && triplet.srcAttr.srcVertex != -1L) {
            // Check if the dstVertex is inactive
            if (triplet.dstAttr.superState != SuperState.Inactive) {
              // Check if the source vertex has visited this vertex yet
              if (triplet.srcAttr.decisionTree.traverseTo(triplet.dstId) == null) {
                // Check if dstVertex has any more moves
                // Add this vertex to srcVertex's decision tree
                if (triplet.srcAttr.addToPath(triplet.dstId, triplet.srcId)) {
                  Seq[(VertexId, ShortestPathState)]((triplet.srcId, triplet.srcAttr), (triplet.dstId, triplet.srcAttr))
                } else {
                  Seq[(VertexId, ShortestPathState)]()
                }
              } else {
                // Check if dstVertex has found a path to the destination
                val dstCheck = triplet.dstAttr.decisionTree.traverseTo(vertexId)
                if (dstCheck != null) {
                  // Get the shortest path and replicate it to this vertex's decision tree
                  val shortestPath = triplet.dstAttr.decisionTree.shortestPathTo(vertexId)
                  val shortestPathArray = shortestPath.toArray
                  for (x <- shortestPathArray; y = shortestPathArray.indexOf(x)) {
                    if (y < shortestPath.length - 1)
                      triplet.srcAttr.decisionTree.traverseTo(x).addLeaf(shortestPathArray(y + 1))
                  }
                }
                Seq[(VertexId, ShortestPathState)]()
              }
            } else {
              Seq[(VertexId, ShortestPathState)]()
            }
          } else {
            Seq[(VertexId, ShortestPathState)]()
          }
        } else {
          Seq[(VertexId, ShortestPathState)]()
        }
      }

    results.flatMap(a => a).toIterator
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
class ShortestPathState(val srcVertex: VertexId, val dstVertex: Seq[VertexId]) extends Serializable {

  var neighborsIn : Seq[VertexId] = Seq()
  var neighborsOut : Seq[VertexId] = Seq()
  var stage : Int = 0

  /**
   * The [[SuperState]] is used to track whether or not this vertex has an optimized path to the destination
   */
  var superState : SuperState = SuperState.Active

  /**
   * A [[DecisionTree]] is used to provide a stateful mechanism for finding the shortest path to the destination vertex
   */
  val decisionTree = new DecisionTree[VertexId](srcVertex, mutable.HashMap[VertexId, DecisionTree[VertexId]]())

  def getShortestPaths : Seq[(VertexId, Seq[Seq[VertexId]])] = {
    // Check if there is a path to the destination
    dstVertex.map(a => (a, decisionTree.allShortestPathsTo(a)))
  }

  /**
   * Adds a new branch to the decision tree for this vertex representing the paths it has already explored
   * @param to is the new leaf to add to the branch
   * @param from is the branch to add the new leaf on
   * @return a [[Boolean]] representing whether or not the operation was successful
   */
  def addToPath(to: VertexId, from: VertexId) : Boolean = {


    val endNode = decisionTree.traverseTo(from)
    if (endNode != null) {
      if (endNode.traverseTo(to) == null) {
        return endNode.addLeaf(to) != null
      } else false
    } else false


    val thisEndNode = decisionTree.traverseTo(from)

    if (thisEndNode != null) {
      if (thisEndNode.traverseTo(to) == null) {
        thisEndNode.addLeaf(to) != null
      } else {
        false
      }
    } else false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[ShortestPathState]

  override def hashCode(): Int = {
    val state = Seq(decisionTree, srcVertex, dstVertex)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def equals(other: Any): Boolean = other match {
    case that: ShortestPathState =>
      (that canEqual this) &&
        decisionTree == that.decisionTree &&
        srcVertex == that.srcVertex &&
        dstVertex == that.dstVertex
    case _ => false
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
class DecisionTree[VD](val root : VD, var graph : mutable.HashMap[VD, DecisionTree[VD]]) extends Serializable {

  /**
   * The branches for this decision tree, consisting of a set of leaf vertices that have context to a traversable branch
   */
  var branches : scala.collection.mutable.SynchronizedSet[DecisionTree[VD]] = new scala.collection.mutable.HashSet[DecisionTree[VD]]() with mutable.SynchronizedSet[DecisionTree[VD]]

  var distance : Int = -1

  def cloneTree(newGraph: mutable.HashMap[VD, DecisionTree[VD]]): DecisionTree[VD] = {
    val cloned = new DecisionTree[VD](root, newGraph)

    // Clone all branches
    cloned.branches.++=(for (tree <- branches) yield {
      newGraph.getOrElseUpdate(tree.root, tree)
      tree.cloneTree(newGraph)
    })

    cloned
  }

  override def clone(): DecisionTree[VD] = {
    val cloned = new DecisionTree[VD](root, graph.clone())

    // Clone all branches
    cloned.branches.++=(for (tree <- branches) yield {
      tree.clone()
    })

    cloned
  }

  def addBranch(branch : DecisionTree[VD]) : DecisionTree[VD] = {
    addBranches(branch.branches)

    branch
  }

  def addBranches(branches : scala.collection.mutable.SynchronizedSet[DecisionTree[VD]]) = {

    for (branch <- branches ) yield {
      // Get or put the decision tree into the global decision tree hash map
      val treeBranch = graph.getOrElse(branch.root, null)

      // Recursively update the graph
      if(treeBranch == null) {
        val newBranch : DecisionTree[VD] = branch.clone()
        newBranch.graph = this.graph
        this.graph synchronized { this.graph.put(newBranch.root, newBranch) }

        this.addLeaf(newBranch.root)

        branch.branches.foreach(a => addBranch(a))
      } else {
        treeBranch.addBranch(branch)
        // Update tree branch
        this.addLeaf(treeBranch.root)
      }

      branch
    }

  }

  /**
   * Adds a leaf vertex to this branch
   * @param leaf is the vertex id of the new leaf
   * @return a new branch for this leaf
   */
  def addLeaf(leaf : VD) : DecisionTree[VD] = {
    // Get or put the decision tree into the global decision tree hash map
    val branch = graph.getOrElseUpdate(leaf, synchronized { new DecisionTree[VD](leaf, graph) })
    this.branches.add(branch)
    branch
  }

  def addNode(leaf : VD) : DecisionTree[VD] = {
    // Get or put the decision tree into the global decision tree hash map
    val branch = graph.getOrElseUpdate(leaf, synchronized { new DecisionTree[VD](leaf, graph) })
    branch
  }

  def getNode(leaf : VD) : DecisionTree[VD] = {
    // Get or put the decision tree into the global decision tree hash map
    val branch = graph.getOrElse(leaf, null)
    branch
  }

  def traverseTo(item : VD) : DecisionTree[VD] = {
    traverseTo(item, Seq[VD]())
  }

  /**
   * Traverses the decision tree until it finds a branch that matches a supplied vertex id
   * @param item is the vertex id of the item to traverse to
   * @return a branch for the desired vertex
   */
  def traverseTo(item : VD, seq: Seq[VD]) : DecisionTree[VD] = {

    if(seq.contains(this.root)) {
      return null
    }

    if(item == root) {
      if(seq.contains(this.root)) {
        return null
      }

      this
    } else {
      val result = {
        if(seq.find(a => a.equals(item)).getOrElse(null) == null) {
          for (branch <- branches ; x = branch.traverseTo(item, seq ++ Seq[VD](this.root)) if x != null && !seq.contains(x)) yield x
        } else {
          Seq[DecisionTree[VD]]()
        }
      }.take(1)
        .find(a => a != null)
        .getOrElse(null)

      if(seq.contains(this.root)) {
        return null
      }

      result
    }
  }

  def shortestPathTo(item : VD) : Seq[VD] = {
    shortestPathTo(item, Seq[VD]())
  }

  /**
   * Gets the shortest path to the item or else returns null
   * @param item is the id of the vertex to traverse to
   * @return the shortest path as a sequence of [[VD]]
   */
  def shortestPathTo(item : VD, seq: Seq[VD]) : Seq[VD] = {

    if(seq.contains(this.root)) {
      return null
    }

    if(item == root) {
      if(seq.contains(this.root)) {
        return null
      }
      Seq[VD](this.root)
    } else {
      val result = {
        for (branch <- branches ; x = branch.shortestPathTo(item, seq ++ Seq[VD](this.root)) if x != null && !seq.contains(x)) yield x
      }.toSeq.sortBy(b => b.length).take(1)
        .find(a => a != null)
        .getOrElse(null)

      if(seq.contains(this.root)) {
        return null
      }

      result match {
        case x: Seq[VD] => x.+:(root)
        case x => null
      }
    }
  }

  object branchOrdering extends Ordering[DecisionTree[VD]] {
    def compare(a:DecisionTree[VD], b:DecisionTree[VD]) = a.distance compare b.distance
  }

  def allShortestPathsTo(item : VD, distance : Int, depth : Int) : Array[Seq[VD]] = {
    if(depth > distance) return null

    if(item == root) {
        Array(Seq[VD](this.root))
    } else {
      val result = {
        if (branches.size > 0) {
          val minV = branches.min(branchOrdering)
          for (branch <- branches.filter(b => b.distance == minV.distance); x = branch.allShortestPathsTo(item, distance, depth + 1) if x != null) yield x
        } else {
          null
        }
      }

      result match {
        case x: scala.collection.mutable.HashSet[Array[Seq[VD]]] => {
          result.flatMap(a => a).map(a => Seq[VD](this.root) ++ a).toArray
        }
        case x => null
      }
    }
  }

  def allShortestPathsTo(item : VD, graphRef : Array[(VertexId, SPMap)]) : Seq[Seq[VD]] = {
    // Set the distances on all the vertices
    graphRef.foreach(a => {
      val distanceForVertex = a._2.getOrElse(item.asInstanceOf[VertexId], null)
      if (distanceForVertex != null) {
        val dt = graph.getOrElse(a._1.asInstanceOf[VD], null)
        if (dt != null) {
          dt.distance = distanceForVertex.asInstanceOf[Int]
          graph.put(dt.root, dt)
          this.graph = graph
        }
      }
    })
    allShortestPaths(item, 1, 0)
  }

  def allShortestPathsTo(item : VD) : Seq[Seq[VD]] = {
    allShortestPaths(item, 1, 0)
  }

  def allShortestPaths(item : VD, minDistance : Int, maxDistance : Int) : Seq[Seq[VD]] = {
    val shortestPath = {
      if(distance == -1) {
        return null
      } else {
        distance
      }
    }

    if(shortestPath > minDistance) {
      val thisShortestPath = allShortestPathsTo(item, shortestPath, 0)
      if(thisShortestPath.length > 0) {
        thisShortestPath
      } else {
        null
      }
    } else {
      null
    }
  }

  def toGraph: com.github.mdr.ascii.graph.Graph[String] = {
    toGraph(Seq[VD](), scala.collection.mutable.Set[(String, String)](), scala.collection.mutable.Set[String]())
  }

  /**
   * Converts a [[DecisionTree]] to a [[com.github.mdr.ascii.graph.Graph]] which can be rendered in ASCII art
   * @return a [[com.github.mdr.ascii.graph.Graph]] that can be visualized as ASCII art
   */
  def toGraph(seq : Seq[VD], edges: scala.collection.mutable.Set[(String, String)], vertices: scala.collection.mutable.Set[String]): com.github.mdr.ascii.graph.Graph[String] = {
    vertices.add(root.toString)
    branches.foreach(a => if(!vertices.contains(a.root.toString)) vertices.add(a.root.toString))
    branches.map(a => (root.toString, a.root.toString)).foreach(a => edges.add(a))
    val thisBranches = branches

    thisBranches.foreach(a => {
      if(!seq.contains(a.root)) {
        val thisGraph = a.toGraph(seq ++ Seq[VD](a.root), edges, vertices)
        thisGraph.vertices.filter(b => b != a.root).foreach(b => vertices.add(b))
        thisGraph.edges.foreach(b => edges.add(b))
      }
    })

    com.github.mdr.ascii.graph.Graph(vertices.toList.toSet, edges.toList)
  }

  /**
   * Renders the [[DecisionTree]] in ASCII art
   * @return a [[String]] that has a graph layout visualization in ASCII art of the [[DecisionTree]]
   */
  def renderGraph: String = {
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

