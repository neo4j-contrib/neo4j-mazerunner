package models;

import java.io.BufferedWriter;
import java.io.IOException;

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
public abstract class ParallelWriterConfiguration<T> {
    int start;
    int length;
    BufferedWriter bufferedWriter;
    Integer fullSize;
    Integer reportBlockSize;

    public ParallelWriterConfiguration(int start, int length, BufferedWriter bufferedWriter, Integer fullSize, Integer reportBlockSize) {
        this.start = start;
        this.length = length;
        this.bufferedWriter = bufferedWriter;
        this.fullSize = fullSize;
        this.reportBlockSize = reportBlockSize;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public BufferedWriter getBufferedWriter() {
        return bufferedWriter;
    }

    public void setBufferedWriter(BufferedWriter bufferedWriter) {
        this.bufferedWriter = bufferedWriter;
    }

    public Integer getFullSize() {
        return fullSize;
    }

    public void setFullSize(Integer fullSize) {
        this.fullSize = fullSize;
    }

    public Integer getReportBlockSize() {
        return reportBlockSize;
    }

    public void setReportBlockSize(Integer reportBlockSize) {
        this.reportBlockSize = reportBlockSize;
    }

    public void writeBlock(T block) {
        try {
            bufferedWriter.write(block.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ParallelWriterConfiguration<T> clone() throws CloneNotSupportedException {
        return (ParallelWriterConfiguration<T>)super.clone();
    }
}
