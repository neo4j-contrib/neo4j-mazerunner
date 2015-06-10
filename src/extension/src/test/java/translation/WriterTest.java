package translation;

import com.google.gson.Gson;
import config.ConfigurationLoader;
import hdfs.FileUtil;
import jobs.PartitionedAnalysis;
import junit.framework.TestCase;
import messaging.Worker;
import models.JobRequestType;
import models.PartitionDescription;
import models.ProcessorMessage;
import models.ProcessorMode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

public class WriterTest extends TestCase {

    @Test
    public void testWriteToHadoop() throws Exception {
        GraphDatabaseService db = setUpDb();

        // Use test configurations
        ConfigurationLoader.testPropertyAccess = true;

        createSampleGraph(db);

        Writer.exportSubgraphToHDFS(db);
    }

    private void createSampleGraph(GraphDatabaseService db) {

        List<Node> nodes = new ArrayList<>();

        int max = 200;
        Transaction tx = db.beginTx();
        Node partitionNode = db.createNode();
        partitionNode.addLabel(DynamicLabel.label("Category"));
        tx.success();
        tx.close();
        int count = 0;
        int partitionBlockCount = 50;

        tx = db.beginTx();
        // Create nodes
        for (int i = 0; i < max; i++) {

            nodes.add(db.createNode());
            nodes.get(i).addLabel(DynamicLabel.label("Node"));
            partitionNode.createRelationshipTo(nodes.get(i), withName("HAS_CATEGORY"));
            count++;
            if(count >= partitionBlockCount && i != max - 1) {

                count = 0;
                partitionNode = db.createNode();
                partitionNode.addLabel(DynamicLabel.label("Category"));

                tx.success();
                tx.close();
                tx = db.beginTx();

                System.out.println(i);
            }
        }

        tx.success();
        tx.close();

        tx = db.beginTx();
        // Create PageRank test graph
        for (int i = 0; i < (max / 2) - 1; i++) {
            nodes.get(i).createRelationshipTo(nodes.get(i + (max / 2)), withName("CONNECTED_TO"));
            nodes.get(i + (max / 2)).createRelationshipTo(nodes.get(i + 1), withName("CONNECTED_TO"));
            if(count >= partitionBlockCount / 2 && i != max - 1) {
                tx.success();
                tx.close();
                tx = db.beginTx();

                System.out.println("B: " + i);
            }
            if(i == (max / 2) - 2) {
                nodes.get((i + 1) + (max / 2)).createRelationshipTo(nodes.get(0), withName("CONNECTED_TO"));
                nodes.get(i + 1).createRelationshipTo(nodes.get((max / 2)), withName("CONNECTED_TO"));
            }
        }

        tx.success();
        tx.close();
    }

    @Test
    public void testParallelWriteGraphResultToHDFS() throws Exception {
        GraphDatabaseService db = setUpDb();

        // Use test configurations
        ConfigurationLoader.testPropertyAccess = true;

        createSampleGraph(db);

        Writer.exportSubgraphToHDFSParallel(db);
    }

    @Test
    public void testParallelWriteCypherResultToHDFS() throws Exception {
        GraphDatabaseService db = setUpDb();

        // Use test configurations
        ConfigurationLoader.testPropertyAccess = true;

        createSampleGraph(db);

        Writer.exportCypherQueryToHDFSParallel(db, "MATCH (n1)-[r]->(n2) RETURN id(n1) as v1, id(n2) as v2, rand() as v3", JobRequestType.COLLABORATIVE_FILTERING);
    }

    @Test
    public void testPartitionedAnalysis() throws Exception {
        GraphDatabaseService db = setUpDb();

        // Use test configurations
        ConfigurationLoader.testPropertyAccess = true;

        createSampleGraph(db);

        // Export graph to HDFS and send message to Spark when complete
        PartitionedAnalysis partitionedAnalysis = new PartitionedAnalysis("pagerank", "Category", "HAS_CATEGORY", "CONNECTED_TO", db);
        partitionedAnalysis.analyzePartitions();
    }

    @Test
    public void testCFParallelUpdate() throws Exception {

        GraphDatabaseService db = setUpDb();

        // Use test configurations
        ConfigurationLoader.testPropertyAccess = true;

        createSampleGraph(db);

        Path path = Writer.exportCypherQueryToHDFSParallel(db, "MATCH (n1)-[r]->(n2) RETURN id(n1) as v1, id(n2) as v2, toInt(ROUND((rand() * 10.0))) as v3", JobRequestType.COLLABORATIVE_FILTERING);

        ProcessorMessage processorMessage = new ProcessorMessage(path.toString(), JobRequestType.COLLABORATIVE_FILTERING.toString().toLowerCase(), ProcessorMode.Unpartitioned);

        BufferedReader br = FileUtil.readGraphAdjacencyList(processorMessage);

        // Test parallel update
        Writer.asyncImportCollaborativeFiltering(br, db);
    }

    @Test
    public void testParallelUpdate() throws Exception {

        GraphDatabaseService db = setUpDb();

        Transaction tx = db.beginTx();


        // Use test configurations
        ConfigurationLoader.testPropertyAccess = true;

        Node nodePartition = db.createNode();
        nodePartition.addLabel(DynamicLabel.label("Category"));

        PartitionDescription partitionDescription = new PartitionDescription(nodePartition.getId(), "Category");

        // Create sample PageRank result
        String nodeList = "";

        for(int i = 0; i < 100; i++)
        {
            db.createNode();
            nodeList += i + " .001\n";
        }

        tx.success();
        tx.close();

        // Create test path
        String path = ConfigurationLoader.getInstance().getHadoopHdfsUri() + "/test/propertyNodeList.txt";

        writeListFile(path, nodeList);

        ProcessorMessage processorMessage = new ProcessorMessage(path, "pagerank", ProcessorMode.Partitioned);
        processorMessage.setPartitionDescription(partitionDescription);

        BufferedReader br = FileUtil.readGraphAdjacencyList(processorMessage);
        BufferedReader br2 = FileUtil.readGraphAdjacencyList(processorMessage);

        // Test parallel update
        PartitionedAnalysis.updatePartition(processorMessage, br, db);
        PartitionedAnalysis.updatePartition(processorMessage, br2, db);
    }

    /**
     * Write a file to HDFS line by line.
     * @param path The path to the HDFS file to be created.
     * @param nodeList The list of node IDs and properties to update in Neo4j.
     * @throws java.io.IOException
     * @throws java.net.URISyntaxException
     */
    public static void writeListFile(String path, String nodeList) throws IOException, URISyntaxException {
        FileSystem fs = FileUtil.getHadoopFileSystem();
        Path updateFilePath = new Path(path);
        BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(updateFilePath,true)));

        br.write("# Node Property Value List");
        BufferedReader nodeListReader = new BufferedReader(new StringReader(nodeList));

        String line;
        while((line = nodeListReader.readLine()) != null) {
            br.write( "\n" + line);
        }
        br.flush();
        br.close();
    }

    private static GraphDatabaseService setUpDb()
    {
        return new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    public void testSendCFMessage() throws Exception {
        ConfigurationLoader.testPropertyAccess=true;

        // Serialize processor message
        GraphDatabaseService db = setUpDb();

        // Use test configurations
        ConfigurationLoader.testPropertyAccess = true;

        createSampleGraph(db);

        Path path = Writer.exportCypherQueryToHDFSParallel(db, "MATCH (n1)-[r]->(n2) RETURN id(n1) as v1, id(n2) as v2, toInt(ROUND((rand() * 10.0))) as v3", JobRequestType.COLLABORATIVE_FILTERING);

        ProcessorMessage processorMessage = new ProcessorMessage(path.toString(), JobRequestType.COLLABORATIVE_FILTERING.toString().toLowerCase(), ProcessorMode.Unpartitioned);

        Gson gson = new Gson();
        String strMessage = gson.toJson(processorMessage);

        // Send message to the Spark graph processor
        Worker.sendMessage(strMessage);
    }

    public void testSendProcessorMessage() throws Exception {
        ConfigurationLoader.testPropertyAccess=true;

        // Serialize processor message
        ProcessorMessage message = new ProcessorMessage("", "strongly_connected_components", ProcessorMode.Partitioned);
        PartitionDescription partitionDescription = new PartitionDescription((long) 200, "Category");
        message.setPartitionDescription(partitionDescription);
        message.setPath(ConfigurationLoader.getInstance().getHadoopHdfsUri() + Writer.EDGE_LIST_RELATIVE_FILE_PATH);
        Gson gson = new Gson();
        String strMessage = gson.toJson(message);

        // Send message to the Spark graph processor
        Worker.sendMessage(strMessage);
    }
}