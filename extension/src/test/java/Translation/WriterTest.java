package translation;

import config.ConfigurationLoader;
import hdfs.FileUtil;
import junit.framework.TestCase;
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
        Transaction tx = db.beginTx();
        List<Node> nodes = new ArrayList<>();

        int max = 60000;

        // Create nodes
        for (int i = 0; i < max; i++) {
            nodes.add(db.createNode());
            nodes.get(i).addLabel(DynamicLabel.label("Node"));
        }

        // Create PageRank test graph
        for (int i = 0; i < (max / 2) - 1; i++) {
            nodes.get(i).createRelationshipTo(nodes.get(i + (max / 2)), withName("CONNECTED_TO"));
            nodes.get(i + (max / 2)).createRelationshipTo(nodes.get(i + 1), withName("CONNECTED_TO"));
            if(i == (max / 2) - 2) {
                nodes.get((i + 1) + (max / 2)).createRelationshipTo(nodes.get(0), withName("CONNECTED_TO"));
                nodes.get(i + 1).createRelationshipTo(nodes.get((max / 2)), withName("CONNECTED_TO"));
            }
        }

        tx.success();
        tx.close();
    }

    @Test
    public void testParallelWriteToHadoop() throws Exception {
        GraphDatabaseService db = setUpDb();

        // Use test configurations
        ConfigurationLoader.testPropertyAccess = true;

        createSampleGraph(db);

        Writer.exportSubgraphToHDFSParallel(db);
    }

    @Test
    public void testParallelUpdate() throws Exception {

        GraphDatabaseService db = setUpDb();

        Transaction tx = db.beginTx();


        // Use test configurations
        ConfigurationLoader.testPropertyAccess = true;


        // Create sample PageRank result
        String nodeList = "";

        for(int i = 0; i < 50000; i++)
        {
            db.createNode();
            nodeList += i + " .001\n";
        }

        tx.success();
        tx.close();

        // Create test path
        String path = ConfigurationLoader.getInstance().getHadoopHdfsUri() + "/test/propertyNodeList.txt";

        writeListFile(path, nodeList);

        BufferedReader br = FileUtil.readGraphAdjacencyList(path);

        // Test parallel update
        Writer.asyncUpdate(br, db);
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
        return new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase();
    }
}