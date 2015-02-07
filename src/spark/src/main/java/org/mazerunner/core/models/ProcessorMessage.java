package org.mazerunner.core.models;

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

/**
 * The ProcessorMessage class is used to distribute messages between the graph processor and Neo4j.
 */
public class ProcessorMessage {
    private String path;
    private String analysis;
    private ProcessorMode mode;
    private PartitionDescription partitionDescription;

    public ProcessorMessage(String path, String analysis, ProcessorMode mode) {
        this.path = path;
        this.analysis = analysis;
        this.mode = mode;
    }

    /**
     * Get the HDFS path.
     * @return The path to the HDFS file for this process.
     */
    public String getPath() {
        return path;
    }

    /**
     * Set the HDFS path.
     * @param path The path to the HDFS file for this process.
     */
    public void setPath(String path) {
        this.path = path;
    }

    /**
     * Get the analysis type.
     * @return The key for the analysis type.
     */
    public String getAnalysis() {
        return analysis;
    }

    /**
     * Set the analysis type.
     * @param analysis The key for the analysis type.
     */
    public void setAnalysis(String analysis) {
        this.analysis = analysis;
    }

    /**
     * Get the mode type.
     * @return The mode type for the analysis, either partitioned or unpartitioned.
     */
    public ProcessorMode getMode() {
        return mode;
    }

    /**
     * Set the mode type.
     * @param mode The mode type represents whether the analysis should be partitioned.
     */
    public void setMode(ProcessorMode mode) {
        this.mode = mode;
    }

    /**
     * Get the description for the partitioned analysis.
     * @return Returns a description for the queried partition.
     */
    public PartitionDescription getPartitionDescription() {
        return partitionDescription;
    }

    /**
     * Set the partition description for an analysis. Preserves information related to
     * the analysis being performed on the current partition.
     * @param partitionDescription A set of fields that describe the partition being analyzed.
     */
    public void setPartitionDescription(PartitionDescription partitionDescription) {
        this.partitionDescription = partitionDescription;
    }
}
