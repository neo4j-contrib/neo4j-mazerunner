package org.mazerunner.core.hdfs;

import junit.framework.TestCase;

public class FileUtilTest extends TestCase {

    public void testWritePropertyGraphUpdate() throws Exception {
        // Create sample PageRank result
        String nodeList =
                "0 .001\n" +
                "1 .002\n" +
                "3 .003";

        // Create test path
        String path = "hdfs://0.0.0.0:9000/test/propertyNodeList.txt";

        // Test writing the PageRank result to HDFS path
        FileUtil.writePropertyGraphUpdate(path, nodeList);

        // Validate node list
        assertEquals(FileUtil.readGraphAdjacenyList(path), "# Node Property Value List" + "\n" + nodeList);
    }
}