package translation;

import config.ConfigurationLoader;
import hdfs.FileUtil;
import messaging.Worker;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
    public static Integer updateCounter = 0;
    public static Integer counter = 0;

    public static void startAgentJob(GraphDatabaseService db) throws IOException, URISyntaxException {

        // Export the subgraph to HDFS
        Path pt = exportSubgraphToHDFSParallel(db);

        // Send message to the Spark graph processor
        Worker.sendMessage(pt.toString());
    }

    public static Path exportSubgraphToHDFSParallel(GraphDatabaseService db) throws  IOException, URISyntaxException {
        FileSystem fs = FileUtil.getHadoopFileSystem();
        Path pt = new Path(ConfigurationLoader.getInstance().getHadoopHdfsUri() + EDGE_LIST_RELATIVE_FILE_PATH);
        BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt)));

        Integer reportBlockSize = 20000;
        Transaction tx = db.beginTx();

        // Get all nodes in the graph
        Iterable<Node> nodes = GlobalGraphOperations.at(db)
                .getAllNodes();

        br.write("# Adacency list" + "\n");

        List<Spliterator<Node>> spliteratorList = new ArrayList<>();
        boolean hasSpliterator = true;
        Spliterator<Node> nodeSpliterator = nodes.spliterator();

        while(hasSpliterator)
        {
            Spliterator<Node> localSpliterator = nodeSpliterator.trySplit();
            hasSpliterator = localSpliterator != null;
            if(hasSpliterator)
                spliteratorList.add(localSpliterator);
        }

        tx.success();
        tx.close();

        counter = 0;

        if(spliteratorList.size() > 4) {
            // Fork join
            ParallelWriter parallelWriter = new ParallelWriter(spliteratorList.toArray(new Spliterator[spliteratorList.size()]), 0, spliteratorList.size(), db, br, spliteratorList.size(), reportBlockSize);
            ForkJoinPool pool = new ForkJoinPool();
            pool.invoke(parallelWriter);
        }
        else
        {
            // Sequential
            spliteratorList.forEach(sl -> sl.forEachRemaining(n -> {
                try {
                    writeBlockForNode(n, db, br, reportBlockSize);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }));
        }

        System.out.println("Mazerunner Export Status: " + MessageFormat.format("{0,number,#.##%}", 1.0));

        br.flush();
        br.close();

        return pt;
    }

    public static void writeBlockForNode(Node n, GraphDatabaseService db, BufferedWriter bufferedWriter, int reportBlockSize) throws IOException {
        Transaction tx = db.beginTx();
        Iterable<Relationship> rels = n.getRelationships(withName(ConfigurationLoader.getInstance().getMazerunnerRelationshipType()), Direction.OUTGOING);
        Stream<Relationship> relStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(rels.iterator(), Spliterator.NONNULL), false);
        relStream.forEach(rel ->
        {
            try {
                String line = rel.getStartNode().getId() + " " + rel.getEndNode().getId();
                bufferedWriter.write(line + "\n");
                Writer.counter++;
                if (Writer.counter % reportBlockSize == 0) {
                    // Report status
                    System.out.println("Records exported: " + Writer.counter);
                }
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
        });
        tx.success();
        tx.close();
        bufferedWriter.flush();
    }

    public static void updateBlockForRow(String line, GraphDatabaseService db, int reportBlockSize) {
        if (line != null && !line.startsWith("#")) {
            String[] rowVal = line.split("\\s");
            Long nodeId = Long.parseLong(rowVal[0]);
            Double weight = Double.parseDouble(rowVal[1]);
            db.getNodeById(nodeId).setProperty("weight", weight);
            Writer.updateCounter++;
            if(Writer.updateCounter % reportBlockSize == 0)
            {
                System.out.println("Nodes updated: " + Writer.updateCounter);
            }
        }
    }

    public static void asyncUpdate(BufferedReader bufferedReader, GraphDatabaseService graphDb) throws IOException {

        Integer reportBlockSize = 10000;

        Stream<String> iterator = bufferedReader.lines();

        List<Spliterator<String>> spliteratorList = new ArrayList<>();
        boolean hasSpliterator = true;
        Spliterator<String> nodeSpliterator = iterator.spliterator();

        while (hasSpliterator) {
            Spliterator<String> localSpliterator = nodeSpliterator.trySplit();
            hasSpliterator = localSpliterator != null;
            if (hasSpliterator)
                spliteratorList.add(localSpliterator);
        }

        counter = 0;
        if(spliteratorList.size() > 4) {
            // Fork join
            ParallelBatchTransaction parallelBatchTransaction = new ParallelBatchTransaction(spliteratorList.toArray(new Spliterator[spliteratorList.size()]), 0, spliteratorList.size(), graphDb, reportBlockSize, spliteratorList.size());
            ForkJoinPool pool = new ForkJoinPool();
            pool.invoke(parallelBatchTransaction);
        } else {
            // Sequential
            Transaction tx = graphDb.beginTx();
            spliteratorList.forEach(sl -> sl.forEachRemaining(n -> updateBlockForRow(n, graphDb, reportBlockSize)));
            tx.success();
            tx.close();
        }

        System.out.println("Job completed");
    }

    public static Path exportSubgraphToHDFS(GraphDatabaseService db) throws IOException, URISyntaxException {
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

        int size = IteratorUtil.count(nodes.iterator());

        System.out.println(nodes.spliterator().trySplit().estimateSize());

        // Fork join

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
                    if (pathCount[0] > pathCountBlocks) {
                        pathCount[0] = 0;
                        System.out.println("Mazerunner Export Status: " + MessageFormat.format("{0,number,#%}", ((double) nodeCount[0] / (double) nodeTotal)));
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

        return pt;
    }
}
