package org.mazerunner.core.models;

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
public class PartitionDescription {
    private Long partitionId;
    private String partitionLabel;
    private String groupRelationship;
    private String targetRelationship;

    public String getPartitionLabel() {
        return partitionLabel;
    }

    public void setPartitionLabel(String partitionLabel) {
        this.partitionLabel = partitionLabel;
    }

    public Long getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(Long partitionId) {
        this.partitionId = partitionId;
    }

    public String getTargetRelationship() {
        return targetRelationship;
    }

    public void setTargetRelationship(String targetRelationship) {
        this.targetRelationship = targetRelationship;
    }

    public String getGroupRelationship() {
        return groupRelationship;
    }

    public void setGroupRelationship(String groupRelationship) {
        this.groupRelationship = groupRelationship;
    }

    public PartitionDescription(Long partitionId, String partitionLabel) {
        this.partitionId = partitionId;
        this.partitionLabel = partitionLabel;
    }
}
