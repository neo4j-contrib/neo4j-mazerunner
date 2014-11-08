package translation;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import java.io.BufferedWriter;
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
 * The ParallelWriter class recursively generates a thread pool to concurrently distribute writes to HDFS.
 */
public class ParallelWriter extends RecursiveAction {
    private static final int THREAD_COUNT = 4;
    private Spliterator<Node>[] mSource;
    private GraphDatabaseService db;
    private BufferedWriter bufferedWriter;
    private int mStart;
    private int mLength;
    private Integer fullSize;
    private Integer reportBlockSize;

    /**
     * Instantiate an instance of ParallelBatchTransaction to perform distributed computation.
     * @param src An array of Spliterator`String` objects that will be processed concurrently.
     * @param start The start position of the src array.
     * @param length The length is the mutating size of the Spliterator[] from the original method call context.
     * @param db A reference to the Neo4j graph database service to apply updates to.
     * @param reportBlockSize The maximum number of updates to apply before reporting to the status log.
     * @param fullSize The fullSize is the unchanged size of the initial Spliterator[] from the original method call context.
     * @param bufferedWriter A reference to the BufferedWriter that will be used to write to HDFS.
     */
    public ParallelWriter(Spliterator<Node>[] src, int start, int length, GraphDatabaseService db, BufferedWriter bufferedWriter, Integer fullSize, Integer reportBlockSize) {
        this.mSource = src;
        this.bufferedWriter = bufferedWriter;
        this.db = db;
        this.mStart = start;
        this.mLength = length;
        this.fullSize = fullSize;
        this.reportBlockSize = reportBlockSize;
    }

    @Override
    protected void compute() {
        if (mLength <= (fullSize / THREAD_COUNT)) {
            try {
                computeDirectly();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return;
        }

        int split = mLength / 2;

        invokeAll(new ParallelWriter(mSource, mStart, split, db, bufferedWriter, fullSize, reportBlockSize),
                  new ParallelWriter(mSource, mStart + split, mLength - split, db, bufferedWriter, fullSize, reportBlockSize));
    }

    /**
     * Computes a section of the source array and applies updates to those nodes referenced
     * in each block.
     * @throws IOException
     */
    protected void computeDirectly() throws IOException {


        for(int i = mStart; i < mStart + mLength; i++) {
            mSource[i].forEachRemaining(n -> {
                try {
                    Writer.writeBlockForNode(n, db, bufferedWriter, reportBlockSize);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }



    }


}
