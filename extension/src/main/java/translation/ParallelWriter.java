package translation;

import config.ConfigurationLoader;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;

import java.io.BufferedWriter;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Spliterator;
import java.util.concurrent.RecursiveAction;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

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
public class ParallelWriter extends RecursiveAction {
    private Spliterator<Node>[] mSource;
    private GraphDatabaseService db;
    private BufferedWriter bufferedWriter;
    private int mStart;
    private int mLength;
    private Integer nodeCount = 0;
    private Integer fullSize;

    public ParallelWriter(Spliterator<Node>[] src, int start, int length, GraphDatabaseService db, BufferedWriter bufferedWriter, Integer nodeCount, Integer fullSize) {
        this.mSource = src;
        this.bufferedWriter = bufferedWriter;
        this.db = db;
        this.nodeCount = nodeCount;
        this.mStart = start;
        this.mLength = length;
        this.fullSize = fullSize;
    }

    @Override
    protected void compute() {

        if (mLength < sThreshold) {
            try {
                computeDirectly();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return;
        }

        int split = mLength - 1;

        invokeAll(new ParallelWriter(mSource, mStart, split, db, bufferedWriter, nodeCount, fullSize),
                new ParallelWriter(mSource, mStart + split, mLength - split,
                        db, bufferedWriter, nodeCount, fullSize));
    }

    protected static int sThreshold = 2;

    protected void computeDirectly() throws IOException {

        Transaction tx = db.beginTx();
        for(int i = mStart; i < mStart + mLength; i++) {
            mSource[i].forEachRemaining(n -> {
                Iterable<org.neo4j.graphdb.Path> nPaths = db.traversalDescription()
                        .depthFirst()
                        .relationships(withName(ConfigurationLoader.getInstance().getMazerunnerRelationshipType()), Direction.OUTGOING)
                        .evaluator(Evaluators.fromDepth(1))
                        .evaluator(Evaluators.toDepth(1))
                        .traverse(n);

                for (org.neo4j.graphdb.Path path : nPaths) {
                    try {
                        String line = path.startNode().getId() + " " + path.endNode().getId();
                        bufferedWriter.write(line + "\n");
                    } catch (Exception ex) {
                        System.out.println(ex.getMessage());
                    }
                }
                Writer.counter++;
            });
        }

        bufferedWriter.flush();

        tx.success();
        tx.close();

        // Report status
        System.out.println("Mazerunner Export Status: " + MessageFormat.format("{0,number,#%}", ((double) Writer.counter / (double) fullSize)));
    }
}
