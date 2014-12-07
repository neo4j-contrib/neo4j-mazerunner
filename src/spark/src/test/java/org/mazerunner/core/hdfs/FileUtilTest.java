package org.mazerunner.core.hdfs;

import junit.framework.TestCase;
import org.mazerunner.core.config.ConfigurationLoader;
import org.mazerunner.core.models.ProcessorMessage;
import org.mazerunner.core.processor.GraphProcessor;

import java.util.ArrayList;
import java.util.Arrays;

public class FileUtilTest extends TestCase {

    public void testWritePropertyGraphUpdate() throws Exception {

        ConfigurationLoader.testPropertyAccess=true;

        // Create sample PageRank result
        String nodeList =
                "0 .001\n" +
                "1 .002\n" +
                "3 .003";

        // Create test path
        String path = ConfigurationLoader.getInstance().getHadoopHdfsUri() + "/test/propertyNodeList.txt";

        // Test writing the PageRank result to HDFS path
        FileUtil.writePropertyGraphUpdate(new ProcessorMessage(path, GraphProcessor.PAGERANK),
                new ArrayList<>(Arrays.asList(
                    "0 .001\n",
                    "1 .002\n",
                    "3 .003"
                )));

        // Validate node list
        assertEquals(FileUtil.readHdfsFile(path), "# Node Property Value List" + "\n" + nodeList);
    }
}