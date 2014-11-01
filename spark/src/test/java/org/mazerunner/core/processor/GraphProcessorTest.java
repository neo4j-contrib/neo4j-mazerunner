package org.mazerunner.core.processor;

import junit.framework.TestCase;
import org.junit.Test;
import org.mazerunner.core.config.ConfigurationLoader;
import org.mazerunner.core.hdfs.FileUtil;

public class GraphProcessorTest extends TestCase {

    @Test
    public void testProcessEdgeList() throws Exception {

        String nodeList =
                "0 1\n" +
                "1 3\n" +
                "3 0";

        // Create test path
        String path = ConfigurationLoader.getInstance().getHadoopHdfsUri() + "/test/edgeList.txt";

        // Test writing the PageRank result to HDFS path
        FileUtil.writeListFile(path, nodeList);

        GraphProcessor.processEdgeList(path);
    }
}