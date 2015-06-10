package models;

import org.neo4j.graphdb.*;
import translation.Writer;

import java.io.BufferedWriter;

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
public class GraphWriter extends ParallelWriterConfiguration<Node> {

    GraphDatabaseService db;
    String relationshipType;

    public GraphWriter(int start, int length, BufferedWriter bufferedWriter, Integer fullSize, Integer reportBlockSize) {
        super(start, length, bufferedWriter, fullSize, reportBlockSize);
    }

    public GraphWriter(int start, int length, BufferedWriter bufferedWriter, Integer fullSize, Integer reportBlockSize, GraphDatabaseService db, String relationshipType) {
        super(start, length, bufferedWriter, fullSize, reportBlockSize);
        this.db = db;
        this.relationshipType = relationshipType;
    }

    public GraphDatabaseService getDb() {
        return db;
    }

    public void setDb(GraphDatabaseService db) {
        this.db = db;
    }

    public String getRelationshipType() {
        return relationshipType;
    }

    public void setRelationshipType(String relationshipType) {
        this.relationshipType = relationshipType;
    }

    @Override
    public void writeBlock(Node block) {
        Transaction tx = db.beginTx();
        for (Relationship relationship : block.getRelationships(withName(relationshipType), Direction.OUTGOING)) {
            try {
                String line = relationship.getStartNode().getId() + " " + relationship.getEndNode().getId();
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
        tx.close();
    }

    @Override
    public ParallelWriterConfiguration<Node> clone() throws CloneNotSupportedException {
        return new GraphWriter(start, length, bufferedWriter, fullSize, reportBlockSize, db, relationshipType);
    }
}
