package org.mazerunner.core.messaging;

import junit.framework.TestCase;
import org.mazerunner.core.processor.GraphProcessor;

public class SenderTest extends TestCase {

    public void testSendMessage() throws Exception {
        Sender.sendMessage(GraphProcessor.PROPERTY_GRAPH_UPDATE_PATH);
    }
}