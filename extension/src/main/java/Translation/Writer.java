package Translation;

import Messaging.Worker;
import config.ConfigurationLoader;
import hdfs.FileUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.text.MessageFormat;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

/**
 * Copyright (C) 2014 Kenny Bastani
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
public class Writer {

    private static final String EDGE_LIST_RELATIVE_FILE_PATH = "/neo4j/mazerunner/edgeList.txt";

    public static void exportSubgraphToHDFS(GraphDatabaseService db) throws IOException, URISyntaxException {
        FileSystem fs = FileUtil.getHadoopFileSystem();
        Path pt = new Path(ConfigurationLoader.getInstance().getHadoopHdfsUri() + EDGE_LIST_RELATIVE_FILE_PATH);
        BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt)));

        Transaction tx = db.beginTx();

        // Get all nodes in the graph
        Iterable<Node> nodes = GlobalGraphOperations.at(db)
                .getAllNodes();

        br.write("# Adacency list" + "\n");

        int nodeTotal = IteratorUtil.count(nodes);
        final int[] nodeCount = {0};
        final int[] pathCount = {0};
        int pathCountBlocks = 10000;

        nodes.iterator().forEachRemaining(n -> {
            // Filter nodes by all paths connected by the relationship type described in the configuration properties
            Iterable<org.neo4j.graphdb.Path> nPaths = db.traversalDescription()
                    .depthFirst()
                    .relationships(withName(ConfigurationLoader.getInstance().getMazerunnerRelationshipType()), Direction.OUTGOING)
                    .evaluator(Evaluators.fromDepth(1))
                    .evaluator(Evaluators.toDepth(1))
                    .traverse(n);

            for (org.neo4j.graphdb.Path path : nPaths) {
                try {
                    String line = path.startNode().getId() + " " + path.endNode().getId();
                    br.write(line + "\n");
                    pathCount[0]++;
                    if(pathCount[0] > pathCountBlocks) {
                        pathCount[0] = 0;
                        System.out.println("Mazerunner Export Status: " + MessageFormat.format("{0,number,#%}", ((double)nodeCount[0] / (double)nodeTotal)));
                    }
                } catch (Exception ex) {
                    System.out.println(ex.getMessage());
                }
            }
            nodeCount[0]++;
        });

        System.out.println("Mazerunner Export Status: " + MessageFormat.format("{0,number,#.##%}", 1.0));

        br.flush();
        br.close();
        tx.success();
        tx.close();

        // Send message to the Spark graph processor
        Worker.sendMessage(pt.toString());
    }
}
