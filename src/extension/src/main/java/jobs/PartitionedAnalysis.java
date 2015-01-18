package jobs;

import models.PartitionDescription;
import models.ProcessorMessage;
import org.apache.hadoop.fs.Path;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;
import translation.Writer;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;

/**
 * Copyright (C) 2014 Kenny Bastani
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
public class PartitionedAnalysis {

    private String type;
    private String label;
    private String groupRelationship;
    private String targetRelationship;
    private GraphDatabaseService graphDatabaseService;

    public PartitionedAnalysis(String type, String label, String groupRelationship, String targetRelationship, GraphDatabaseService graphDatabaseService) {
        this.type = type;
        this.label = label;
        this.groupRelationship = groupRelationship;
        this.targetRelationship = targetRelationship;
        this.graphDatabaseService = graphDatabaseService;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getGroupRelationship() {
        return groupRelationship;
    }

    public void setGroupRelationship(String groupRelationship) {
        this.groupRelationship = groupRelationship;
    }

    public String getTargetRelationship() {
        return targetRelationship;
    }

    public void setTargetRelationship(String targetRelationship) {
        this.targetRelationship = targetRelationship;
    }


    public GraphDatabaseService getGraphDatabaseService() {
        return graphDatabaseService;
    }

    public void setGraphDatabaseService(GraphDatabaseService graphDatabaseService) {
        this.graphDatabaseService = graphDatabaseService;
    }

    public void analyzePartitions() {
        // Query the nodes by label, which each set becomes a partition for analysis
        Transaction tx = graphDatabaseService.beginTx();

        Iterator<Node> partitions = GlobalGraphOperations.at(graphDatabaseService)
                .getAllNodesWithLabel(DynamicLabel.label(label))
                .iterator();

        // For each partition, query the target relationships between the partition
        partitions.forEachRemaining(partition -> {
            PartitionDescription partitionDescription = new PartitionDescription(partition.getId(), label);
            partitionDescription.setGroupRelationship(groupRelationship);
            partitionDescription.setTargetRelationship(targetRelationship);
            try {
                Path pt = Writer.exportPartitionToHDFSParallel(graphDatabaseService, partition, partitionDescription);
                if(pt != null)
                    Writer.dispatchPartitionedJob(graphDatabaseService, type, partitionDescription, pt);
            } catch (IOException | URISyntaxException e) {
                e.printStackTrace();
            }
        });

        tx.success();
        tx.close();
    }

    /**
     * Update the partition from the results of an analysis received from the Mazerunner service.
     * @param processorMessage The description of the analysis job.
     * @param bufferedReader An iterator containing the text results from the analysis job.
     * @param graphDb A context to the Neo4j graph database service.
     * @throws IOException If you mess up, it's because of an IO exception.
     */
    public static void updatePartition(ProcessorMessage processorMessage, BufferedReader bufferedReader, GraphDatabaseService graphDb) throws IOException {
        Writer.asyncPartitionedUpdate(bufferedReader, graphDb, processorMessage);
    }
}
