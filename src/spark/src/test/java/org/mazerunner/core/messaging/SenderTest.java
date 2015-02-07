package org.mazerunner.core.messaging;

import com.google.gson.Gson;
import junit.framework.TestCase;
import org.mazerunner.core.config.ConfigurationLoader;
import org.mazerunner.core.models.ProcessorMessage;
import org.mazerunner.core.models.ProcessorMode;
import org.mazerunner.core.processor.GraphProcessor;

public class SenderTest extends TestCase {

    private static final String EDGE_LIST_RELATIVE_FILE_PATH = "/neo4j/mazerunner/edgeList.txt";

    public void testSendMessage() throws Exception {
        ConfigurationLoader.testPropertyAccess=true;
        ProcessorMessage processorMessage = new ProcessorMessage("", "strongly_connected_components", ProcessorMode.Partitioned);
        processorMessage.setPath(ConfigurationLoader.getInstance().getHadoopHdfsUri() + GraphProcessor.PROPERTY_GRAPH_UPDATE_PATH);
        // Serialize the processor message
        Gson gson = new Gson();
        String message = gson.toJson(processorMessage);

        // Notify Neo4j that a property update list is available for processing
        Sender.sendMessage(message);
    }


}