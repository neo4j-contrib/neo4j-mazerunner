package org.mazerunner.core.processor;

import junit.framework.TestCase;
import org.junit.Test;

public class GraphProcessorTest extends TestCase {

    @Test
    public void testProcessEdgeList() throws Exception {
        GraphProcessor.processEdgeList("hdfs://0.0.0.0:9000/test/edgeList.txt");
    }
}