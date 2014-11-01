package org.mazerunner.core.processor;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.List;

public class GraphProcessorTest extends TestCase {

    @Test
    public void testProcessEdgeList() throws Exception {
        List<Long> nodeList = GraphProcessor.processEdgeList("hdfs://0.0.0.0:9000/test/edgeList.txt");
    }
}