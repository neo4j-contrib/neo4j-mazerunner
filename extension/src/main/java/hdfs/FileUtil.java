package hdfs;

import config.ConfigurationLoader;
import models.ProcessorMessage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

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
public class FileUtil {

    /**
     * Produces a buffered stream reader of a HDFS text file containing an adjacency list.
     * @param processorMessage The path to the text file containing an adjacency list.
     * @return Returns a BufferedReader of the input stream loaded in from the path.
     * @throws IOException
     * @throws URISyntaxException
     */
    public static BufferedReader readGraphAdjacencyList(ProcessorMessage processorMessage) throws IOException, URISyntaxException {
        FileSystem fs = getHadoopFileSystem();
        Path filePath = new Path(processorMessage.getPath());
        FSDataInputStream inputStream = fs.open(filePath);

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        return bufferedReader;
    }

    /**
     * Gets the HDFS file system and loads in local Hadoop configurations.
     * @return Returns a distributed FileSystem object.
     * @throws IOException
     * @throws URISyntaxException
     */
    public static FileSystem getHadoopFileSystem() throws IOException, URISyntaxException {
        Configuration hadoopConfiguration = new Configuration();
        hadoopConfiguration.addResource(new Path(ConfigurationLoader.getInstance().getHadoopHdfsPath()));
        hadoopConfiguration.addResource(new Path(ConfigurationLoader.getInstance().getHadoopSitePath()));
        hadoopConfiguration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        return FileSystem.get(new URI(ConfigurationLoader.getInstance().getHadoopHdfsUri()), hadoopConfiguration);
    }
}
