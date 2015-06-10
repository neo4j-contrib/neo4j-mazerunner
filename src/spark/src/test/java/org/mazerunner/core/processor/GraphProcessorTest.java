package org.mazerunner.core.processor;

import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.util.GraphGenerators;
import org.junit.Test;
import org.mazerunner.core.algorithms;
import org.mazerunner.core.config.ConfigurationLoader;
import org.mazerunner.core.hdfs.FileUtil;
import org.mazerunner.core.models.ProcessorMessage;
import org.mazerunner.core.models.ProcessorMode;
import org.mazerunner.core.programs.DecisionTree;
import scala.collection.JavaConversions;
import scala.collection.mutable.HashMap;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static junit.framework.Assert.assertEquals;

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
    public void testEdgeBetweenness() throws Exception {

        ConfigurationLoader.testPropertyAccess = true;

        // Test case A
        String expectedA = "0 0.0\n1 3.0\n2 4.0\n3 3.0\n4 0.0\n";
        List<String> nodeListA = Arrays.asList("2 1\n", "3 2\n", "4 3\n", "5 4\n", "6 5\n", "7 6\n", "8 7\n", "9 8\n", "10 9\n", "1 10\n", "1 3\n", "2 3\n", "3 5\n", "4 5\n", "5 7\n", "6 7\n", "7 9\n", "8 9\n", "9 1\n", "10 1");
        String actualA = getEdgeBetweennessCentrality("a", nodeListA);
        assertEquals(expectedA, actualA);
    }

    @Test
    public void testBetweennessCentrality() throws Exception {

        ConfigurationLoader.testPropertyAccess = true;

        // Test case A
        String expectedA = "0 0.0\n1 3.0\n2 4.0\n3 3.0\n4 0.0\n";
        List<String> nodeListA = Arrays.asList("0 1\n", "1 2\n", "2 3\n", "3 4\n");
        String actualA = getBetweennessCentrality("a", nodeListA);
        assertEquals(expectedA, actualA);

        // Test case B
        String expectedB = "0 0.0\n1 1.0\n2 0.5\n3 0.0\n4 1.5\n";
        // (4)<--(0)-->(1)-->(2)-->(3)<--(4)<--(1)
        List<String> nodeListB = Arrays.asList("0 1\n", "1 4\n", "0 4\n", "4 3\n", "1 2\n", "2 3");
        String actualB = getBetweennessCentrality("b", nodeListB);
        assertEquals(expectedB, actualB);

        // Test case C
        String expectedC = "1 22.0\n" + "2 4.0\n" + "3 22.0\n" + "4 4.0\n" + "5 22.0\n" + "6 4.0\n" + "7 22.0\n" + "8 4.0\n" + "9 22.0\n" + "10 4.0\n";
        List<String> nodeListC = Arrays.asList("2 1\n", "3 2\n", "4 3\n", "5 4\n", "6 5\n", "7 6\n", "8 7\n", "9 8\n", "10 9\n", "1 10\n", "1 3\n", "2 3\n", "3 5\n", "4 5\n", "5 7\n", "6 7\n", "7 9\n", "8 9\n", "9 1\n", "10 1");
        String actualC = getBetweennessCentrality("c", nodeListC);
        assertEquals(expectedC, actualC);
    }

    @Test
    public void performanceTestBetweennessCentrality() throws Exception {

        ConfigurationLoader.testPropertyAccess = true;

        if (GraphProcessor.javaSparkContext == null)
            GraphProcessor.initializeSparkContext();

        // Generate random graph
        Graph<Object, Object> graph = GraphGenerators.logNormalGraph(GraphProcessor.javaSparkContext.sc(), 100, 0, 4, 5, 423);

        List<String> starGraph = JavaConversions.asJavaCollection(graph.edges().toLocalIterator().toIterable()).stream()
                .map(a -> a.srcId() + " " + a.dstId() + "\n")
                .collect(Collectors.toList());

        System.out.println(starGraph);

        System.out.println(getBetweennessCentrality("performance-graph", starGraph));
    }

    private String getBetweennessCentrality(String test, List<String> nodeList) throws IOException, URISyntaxException {
        // Create test path
        String path = ConfigurationLoader.getInstance().getHadoopHdfsUri() + "/test/" + test + "/edgeList.txt";

        // Test writing the PageRank result to HDFS path
        FileUtil.writeListFile(path, nodeList.iterator());

        if(GraphProcessor.javaSparkContext == null)
            GraphProcessor.initializeSparkContext();
        Iterable<String> results = algorithms.betweennessCentrality(GraphProcessor.javaSparkContext.sc(), path);

        StringBuffer sb = new StringBuffer();

        results.iterator().forEachRemaining(sb::append);

        return sb.toString();
    }

    private String getEdgeBetweennessCentrality(String test, List<String> nodeList) throws IOException, URISyntaxException {
        // Create test path
        String path = ConfigurationLoader.getInstance().getHadoopHdfsUri() + "/test/" + test + "/edgeList.txt";

        // Test writing the PageRank result to HDFS path
        FileUtil.writeListFile(path, nodeList.iterator());

        if(GraphProcessor.javaSparkContext == null)
            GraphProcessor.initializeSparkContext();
        Iterable<String> results = algorithms.edgeBetweenness(GraphProcessor.javaSparkContext.sc(), path);

        StringBuffer sb = new StringBuffer();

        results.iterator().forEachRemaining(sb::append);

        return sb.toString();
    }

    @Test
    public void testVertexPath() throws Exception {
        DecisionTree<Long> tree  = new DecisionTree<>(0L, new HashMap<>());
        tree.traverseTo(0L).addLeaf(1L);
        tree.traverseTo(0L).addLeaf(2L).addLeaf(3L).addLeaf(4L).addLeaf(5L);
        tree.traverseTo(4L).addLeaf(6L);
        tree.traverseTo(4L).addLeaf(7L).addLeaf(8L);
        tree.traverseTo(7L).addLeaf(9L);
        System.out.println(tree.renderGraph());

        System.out.println(tree.shortestPathTo(9L));
    }

    @Test
    public void collaborativeFilteringTest() throws Exception {

        ConfigurationLoader.testPropertyAccess = true;

        // Create test path
        String path = "src/test/resources/recommendation";

        if(GraphProcessor.javaSparkContext == null)
            GraphProcessor.initializeSparkContext();

        algorithms.collaborativeFiltering(GraphProcessor.javaSparkContext.sc(), path).iterator()
                .forEachRemaining(System.out::println);

    }
}