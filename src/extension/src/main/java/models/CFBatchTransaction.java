package models;

import org.neo4j.graphdb.GraphDatabaseService;
import translation.Writer;

import java.io.BufferedReader;

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
public class CFBatchTransaction extends ParallelReaderConfiguration<String>  {

    private GraphDatabaseService graphDatabaseService;

    public CFBatchTransaction(int start, int length, BufferedReader bufferedReader, Integer fullSize, Integer reportBlockSize) {
        super(start, length, bufferedReader, fullSize, reportBlockSize);
    }

    public CFBatchTransaction(int start, int length, BufferedReader bufferedReader, Integer fullSize, Integer reportBlockSize, GraphDatabaseService graphDatabaseService) {
        super(start, length, bufferedReader, fullSize, reportBlockSize);
        this.graphDatabaseService = graphDatabaseService;
    }

    public GraphDatabaseService getGraphDatabaseService() {
        return graphDatabaseService;
    }

    public void setGraphDatabaseService(GraphDatabaseService graphDatabaseService) {
        this.graphDatabaseService = graphDatabaseService;
    }

    @Override
    public void transactBlock(String block) {
        Writer.updateCollaborativeFilteringForRow(block, graphDatabaseService, reportBlockSize);
    }

    @Override
    public ParallelReaderConfiguration<String> clone() throws CloneNotSupportedException {
        return new CFBatchTransaction(start, length, bufferedReader, fullSize, reportBlockSize, graphDatabaseService);
    }
}
