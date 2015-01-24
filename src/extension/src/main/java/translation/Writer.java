package translation;

import com.google.gson.Gson;
import config.ConfigurationLoader;
import hdfs.FileUtil;
import messaging.Worker;
import models.PartitionDescription;
import models.ProcessorMessage;
import models.ProcessorMode;
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
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;

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

    public static final String EDGE_LIST_RELATIVE_FILE_PATH = "/neo4j/mazerunner/jobs/{job_id}/edgeList.txt";
    public static Integer updateCounter = 0;
    public static Integer counter = 0;

    public static void dispatchJob(GraphDatabaseService db, String type) throws IOException, URISyntaxException {

        // Export the subgraph to HDFS
        Path pt = exportSubgraphToHDFSParallel(db);

        // Serialize processor message
        ProcessorMessage message = new ProcessorMessage(pt.toString(), type, ProcessorMode.Unpartitioned);
        Gson gson = new Gson();
        String strMessage = gson.toJson(message);

        // Send message to the Spark graph processor
        Worker.sendMessage(strMessage);
    }

    public static void dispatchPartitionedJob(GraphDatabaseService db, String type, PartitionDescription partitionDescription, Path pt) throws IOException, URISyntaxException {
        // Serialize processor message in partitioned mode
        ProcessorMessage message = new ProcessorMessage(pt.toString(), type, ProcessorMode.Partitioned);
        message.setPartitionDescription(partitionDescription);

        Gson gson = new Gson();
        String strMessage = gson.toJson(message);

        // Send message to the Spark graph processor
        Worker.sendMessage(strMessage);
    }

    public static Path exportPartitionToHDFSParallel(GraphDatabaseService db, Node partitionNode, PartitionDescription partitionDescription) throws IOException, URISyntaxException {
        FileSystem fs = FileUtil.getHadoopFileSystem();
        Path pt = new Path(ConfigurationLoader.getInstance().getHadoopHdfsUri() + EDGE_LIST_RELATIVE_FILE_PATH.replace("{job_id}", partitionDescription.getPartitionId().toString()));
        BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt)));

        Integer reportBlockSize = 20000;

        Transaction tx = db.beginTx();

        ResourceIterable<Node> nodes = db.traversalDescription()
                .depthFirst()
                .relationships(withName(partitionDescription.getGroupRelationship()), Direction.OUTGOING)
                .evaluator(Evaluators.toDepth(1))
                .traverse(partitionNode)
                .nodes();

        if(nodes.iterator().hasNext()) {

            br.write("# Adacency list" + "\n");

            List<Spliterator<Node>> spliteratorList = new ArrayList<>();
            boolean hasSpliterator = true;
            Spliterator<Node> nodeSpliterator = nodes.spliterator();

            while (hasSpliterator) {
                Spliterator<Node> localSpliterator = nodeSpliterator.trySplit();
                hasSpliterator = localSpliterator != null;
                if (hasSpliterator)
                    spliteratorList.add(localSpliterator);
            }


            counter = 0;

            if (spliteratorList.size() > 4) {
                // Fork join
                ParallelWriter parallelWriter = new ParallelWriter(spliteratorList.toArray(new Spliterator[spliteratorList.size()]), 0, spliteratorList.size(), db, br, spliteratorList.size(), reportBlockSize, partitionDescription.getTargetRelationship());
                ForkJoinPool pool = new ForkJoinPool();
                pool.invoke(parallelWriter);
            } else {
                // Sequential
                spliteratorList.forEach(sl -> sl.forEachRemaining(n -> {
                    try {
                        writeBlockForNode(n, db, br, reportBlockSize, partitionDescription.getTargetRelationship());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }));
            }

            System.out.println("Mazerunner Partition Export Status: " + MessageFormat.format("{0,number,#.##%}", 1.0));

            br.flush();
            br.close();

            tx.success();
            tx.close();

            return pt;
        } else {
            return null;
        }
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
            ParallelWriter parallelWriter = new ParallelWriter(spliteratorList.toArray(new Spliterator[spliteratorList.size()]), 0, spliteratorList.size(), db, br, spliteratorList.size(), reportBlockSize, ConfigurationLoader.getInstance().getMazerunnerRelationshipType());
            ForkJoinPool pool = new ForkJoinPool();
            pool.invoke(parallelWriter);
        }
        else
        {
            // Sequential
            spliteratorList.forEach(sl -> sl.forEachRemaining(n -> {
                try {
                    writeBlockForNode(n, db, br, reportBlockSize, ConfigurationLoader.getInstance().getMazerunnerRelationshipType());
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

    public static void writeBlockForNode(Node n, GraphDatabaseService db, BufferedWriter bufferedWriter, int reportBlockSize, String relationshipType) throws IOException {
//        Transaction tx = ((GraphDatabaseAPI)db).tx().unforced().begin();
        Iterator<Relationship> rels = n.getRelationships(withName(relationshipType), Direction.OUTGOING).iterator();
//        Stream<Relationship> relStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(rels.iterator(), Spliterator.NONNULL), true);
        while(rels.hasNext()) {
            try {
                Relationship rel = rels.next();
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
        }
//        tx.success();
//        tx.close();
    }

    /**
     * Applies the result of the analysis as a partitioned value connecting the partition node to the target node.
     * @param line The line from the HDFS text file containing the analysis results.
     * @param db The Neo4j graph database context.
     * @param reportBlockSize The report block size for progress status.
     * @param processorMessage The processor message containing the description of the analysis.
     * @param partitionNode The partition node that will be the source node for creating partitioned relationships to the target node.
     */
    public static void updatePartitionBlockForRow(String line, GraphDatabaseService db, int reportBlockSize, ProcessorMessage processorMessage, Node partitionNode) {
        if (line != null && !line.startsWith("#")) {
            String[] rowVal = line.split("\\s");
            Long nodeId = Long.parseLong(rowVal[0]);
            Double weight = Double.parseDouble(rowVal[1]);
            Node targetNode = db.getNodeById(nodeId);

            Iterator<Relationship> rels = db.traversalDescription()
                    .depthFirst()
                    .relationships(withName(processorMessage.getAnalysis()), Direction.INCOMING)
                    .evaluator(Evaluators.fromDepth(1))
                    .evaluator(Evaluators.toDepth(1))
                    .traverse(targetNode)
                    .relationships()
                    .iterator();

            // Get the relationship to update
            Relationship updateRel = null;

            // Scan the relationships
            while(rels.hasNext() && updateRel == null) {
                Relationship currentRel = rels.next();
                if(currentRel.getStartNode().getId() == partitionNode.getId())
                    updateRel = currentRel;
            }

            // Create or update the relationship for the analysis on the partition
            if(updateRel != null) {
                updateRel.setProperty("value", weight);
            } else {
                Relationship newRel = partitionNode.createRelationshipTo(targetNode, withName(processorMessage.getAnalysis()));
                newRel.setProperty("value", weight);
            }

            Writer.updateCounter++;
            if(Writer.updateCounter % reportBlockSize == 0)
            {
                System.out.println("Nodes updated: " + Writer.updateCounter);
            }
        }
    }

    public static void updateBlockForRow(String line, GraphDatabaseService db, int reportBlockSize, String analysis) {
        if (line != null && !line.startsWith("#")) {
            String[] rowVal = line.split("\\s");
            Long nodeId = Long.parseLong(rowVal[0]);
            Double weight = Double.parseDouble(rowVal[1]);
            db.getNodeById(nodeId).setProperty(analysis, weight);
            Writer.updateCounter++;
            if(Writer.updateCounter % reportBlockSize == 0)
            {
                System.out.println("Nodes updated: " + Writer.updateCounter);
            }
        }
    }

    public static void asyncPartitionedUpdate(BufferedReader bufferedReader, GraphDatabaseService graphDb, ProcessorMessage processorMessage) throws IOException {

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
            ParallelBatchTransaction parallelBatchTransaction =
                    new ParallelBatchTransaction(spliteratorList.toArray(new Spliterator[spliteratorList.size()]),
                            0, spliteratorList.size(), graphDb, reportBlockSize, spliteratorList.size(), processorMessage);

            ForkJoinPool pool = new ForkJoinPool();
            pool.invoke(parallelBatchTransaction);
        } else {
            // Sequential
            Transaction tx = graphDb.beginTx();
            Node partitionNode = graphDb.getNodeById(processorMessage.getPartitionDescription().getPartitionId());
            spliteratorList.forEach(sl -> sl.forEachRemaining(n -> updatePartitionBlockForRow(n, graphDb, reportBlockSize, processorMessage, partitionNode)));
            tx.success();
            tx.close();
        }

        System.out.println("Job completed");
    }

    public static void asyncUpdate(ProcessorMessage analysis, BufferedReader bufferedReader, GraphDatabaseService graphDb) throws IOException {

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
            ParallelBatchTransaction parallelBatchTransaction =
                    new ParallelBatchTransaction(spliteratorList.toArray(new Spliterator[spliteratorList.size()]),
                            0, spliteratorList.size(), graphDb, reportBlockSize, spliteratorList.size(), analysis);

            ForkJoinPool pool = new ForkJoinPool();
            pool.invoke(parallelBatchTransaction);
        } else {
            // Sequential
            Transaction tx = graphDb.beginTx();
            spliteratorList.forEach(sl -> sl.forEachRemaining(n -> updateBlockForRow(n, graphDb, reportBlockSize, analysis.getAnalysis())));
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

        //System.out.println(nodes.spliterator().trySplit().estimateSize());

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
