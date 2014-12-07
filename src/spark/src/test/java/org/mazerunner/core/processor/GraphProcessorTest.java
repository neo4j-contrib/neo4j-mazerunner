package org.mazerunner.core.processor;

import junit.framework.TestCase;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.mazerunner.core.algorithms;
import org.mazerunner.core.config.ConfigurationLoader;
import org.mazerunner.core.hdfs.FileUtil;
import org.mazerunner.core.models.ProcessorMessage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GraphProcessorTest extends TestCase {

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

        GraphProcessor.processEdgeList(new ProcessorMessage(path, GraphProcessor.TRIANGLE_COUNT));
    }

    @Test
    public void testShortestPaths() throws Exception {

        ConfigurationLoader.testPropertyAccess = true;

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
        JavaSparkContext javaSparkContext = GraphProcessor.initializeSparkContext();
        Iterable<String> results = algorithms.closenessCentrality(javaSparkContext.sc(), path);
        results.iterator().forEachRemaining(System.out::print);
    }
}