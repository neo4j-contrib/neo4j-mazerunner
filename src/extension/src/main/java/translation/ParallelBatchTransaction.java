package translation;

import models.ProcessorMessage;
import models.ProcessorMode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.io.IOException;
import java.util.Spliterator;
import java.util.concurrent.RecursiveAction;

/**
 * Copyright (C) 2014 Kenny Bastani
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * The ParallelBatchTransaction class inputs an array of Spliterators,
 * containing information about node updates and applies them concurrently to Neo4j.
 */
public class ParallelBatchTransaction extends RecursiveAction {
    private static final int THREAD_COUNT = 4;
    private Spliterator<String>[] mSource;
    private GraphDatabaseService db;
    private int mStart;
    private int mLength;
    private int reportBlock;
    private int threshold;
    private ProcessorMessage analysis;

    /**
     * Instantiate an instance of ParallelBatchTransaction to perform distributed computation.
     * @param src An array of Spliterator`String` objects that will be processed concurrently.
     * @param start The start position of the src array.
     * @param length The length is the mutating size of the Spliterator[] from the original method call context.
     * @param db A reference to the Neo4j graph database service to apply updates to.
     * @param reportBlock The maximum number of updates to apply before reporting to the status log.
     * @param threshold The threshold is the unchanged size of the initial Spliterator[] from the original method call context.
     * @param analysis
     */
    public ParallelBatchTransaction(Spliterator<String>[] src, int start, int length, GraphDatabaseService db, int reportBlock, int threshold, ProcessorMessage analysis) {
        this.mSource = src;
        this.db = db;
        this.mStart = start;
        this.mLength = length;
        this.reportBlock = reportBlock;
        this.threshold = threshold;
        this.analysis = analysis;
    }

    /**
     * Recursively splits the source array into blocks sized by the THREAD_COUNT.
     */
    @Override
    protected void compute() {

        if (mLength <= (threshold / THREAD_COUNT)) {
            try {
                computeDirectly();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return;
        }

        int split = mLength / 2;

        invokeAll(new ParallelBatchTransaction(mSource, mStart, split, db, reportBlock, threshold, analysis),
                  new ParallelBatchTransaction(mSource, mStart + split, mLength - split, db, reportBlock, threshold, analysis));
    }

    /**
     * Computes a section of the source array and applies updates to those nodes referenced
     * in each block.
     * @throws IOException
     */
    protected void computeDirectly() throws IOException {

        Transaction tx = db.beginTx();

        Node partitionNode = null;

        if(analysis.getMode() == ProcessorMode.Partitioned)
            partitionNode = db.getNodeById(analysis.getPartitionDescription().getPartitionId());

        for (int i = mStart; i < mStart + mLength; i++) {
            final Node finalPartitionNode = partitionNode;
            mSource[i].forEachRemaining(line -> {
                switch (analysis.getMode()) {
                    case Partitioned:
                        Writer.updatePartitionBlockForRow(line, db, reportBlock, analysis, finalPartitionNode);
                        break;
                    case Unpartitioned:
                        Writer.updateBlockForRow(line, db, reportBlock, analysis.getAnalysis());
                        break;
                }

            });
        }

        tx.success();
        tx.close();

    }


}
