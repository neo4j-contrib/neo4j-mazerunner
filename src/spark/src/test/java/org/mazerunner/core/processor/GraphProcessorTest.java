package org.mazerunner.core.processor;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.junit.Test;
import org.mazerunner.core.algorithms;
import org.mazerunner.core.config.ConfigurationLoader;
import org.mazerunner.core.hdfs.FileUtil;
import org.mazerunner.core.models.ProcessorMessage;
import org.mazerunner.core.models.ProcessorMode;
import org.mazerunner.core.programs.DecisionTree;
import scala.Tuple2;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GraphProcessorTest {

    @Test
    public void testProcessEdgeList() throws Exception {

        ConfigurationLoader.testPropertyAccess = true;

        // Create test path
        String path = ConfigurationLoader.getInstance().getHadoopHdfsUri() + "/test/edgeList.txt";

        // Test writing the PageRank result to HDFS path
        FileUtil.writeListFile(path, new ArrayList<>(Arrays.asList(
                "0 1\n",
                "1 3\n",
                "3 0"
        )).iterator());

        GraphProcessor.processEdgeList(new ProcessorMessage(path, GraphProcessor.TRIANGLE_COUNT, ProcessorMode.Unpartitioned));
    }

    @Test
    public void testShortestPaths() throws Exception {

        ConfigurationLoader.testPropertyAccess = true;

//        List<String> nodeList = Arrays.asList(
//                "0 1\n",
//                "1 2\n",
//                "2 3\n",
//                "3 4\n",
//                "4 5\n",
//                "4 7\n",
//                "7 8\n",
//                "8 9\n",
//                "7 10\n",
//                "10 8\n",
//                "8 11\n",
//                "11 12\n",
//                "12 13");

        List<String> nodeList = Arrays.asList(
                "0 1\n",
                "1 2\n",
                "2 3\n",
                "3 4\n",
                "4 5\n",
                "4 7\n",
                "7 8\n",
                "8 9\n",
                "7 10\n",
                "10 8\n",
                "8 11\n",
                "11 12\n",
                "12 13");
        // Create test path
        String path = ConfigurationLoader.getInstance().getHadoopHdfsUri() + "/test/edgeList.txt";

        // Test writing the PageRank result to HDFS path
        FileUtil.writeListFile(path, nodeList.iterator());
        if(GraphProcessor.javaSparkContext == null)
            GraphProcessor.initializeSparkContext();
        Iterable<String> results = algorithms.betweennessCentrality(GraphProcessor.javaSparkContext.sc(), path);
        results.iterator().forEachRemaining(System.out::print);
    }

    @Test
    public void testBetweennessCentrality() throws Exception {

        ConfigurationLoader.testPropertyAccess = true;

        List<String> nodeList = Arrays.asList(
                "0 1\n",
                "1 2\n",
                "2 3");

        /**
         *   +---+    +---+    +---+    +---+
         *   | 0 |--->| 1 |--->| 2 |--->| 3 |
         *   +---+    +---+    +---+    +---+
         */

        /**
         *  Shortest Path results:
         *
         *  0: (0)->(1)->(2)->(3)
         *  1: (1)->(2)->(3)
         *  2: (2)->(3)
         *  3: (3)
         *
         */

/**
 * To collect the shortest path results for all nodes to a single destination node,
 * the following steps must be taken:
 *
 * 0. The state of each node is represented as a decision tree
 *      a. Each node will need to manage a serializable graph data structure
 *      b. Each node can have a super state of active or inactive
 *      c. Active nodes have at least one non-dead branch without the destination node in it
 * 1. The node's state must maintain memory of all shortest paths for each super step iteration
 *      a. Shortest paths are represented as branches of a decision tree for the node's state
 * 2. The node's state must maintain a list of sub-active nodes that will be signaled at the start of the next iteration
 *      a. Sub-active nodes are the nodes on the end of each branch of the decision tree for a node's state
 * 3. Each sub-active node must not be the destination node
 * 4. Each sub-active node will receive a message from an active node and send a message back to the active node with a list of its outgoing edges and its current state
 * 5. Each sub-active node will provide information back to the active node about its active state for the last iteration
 * 6. A node can be dead, mortal, or immortal
 *      a. Dead nodes are inactive
 *      b. Immortal nodes cannot be active but can be sub-active
 *      c. Mortal nodes can be both active and sub-active
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



        // Create test path
        String path = ConfigurationLoader.getInstance().getHadoopHdfsUri() + "/test/edgeList.txt";

        // Test writing the PageRank result to HDFS path
        FileUtil.writeListFile(path, nodeList.iterator());
        JavaSparkContext javaSparkContext = GraphProcessor.initializeSparkContext();
        RDD<Tuple2<Object,Seq<Seq<Object>>>> results = algorithms.singleSourceShortestPath(javaSparkContext.sc(), 3, path);
        System.out.println(results.toDebugString());
    }

    @Test
    public void testVertexPath() throws Exception {
        DecisionTree<Long> tree  = new DecisionTree<>(0L);
        tree.traverseTo(0L).addLeaf(1L);
        tree.traverseTo(0L).addLeaf(2L).addLeaf(3L).addLeaf(4L).addLeaf(5L);
        tree.traverseTo(4L).addLeaf(6L);
        tree.traverseTo(4L).addLeaf(7L).addLeaf(8L);
        tree.traverseTo(7L).addLeaf(9L);
        System.out.println(tree.toString());

        System.out.println(tree.shortestPathTo(9L));


    }
}