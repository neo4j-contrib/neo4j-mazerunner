package org.mazerunner.core.hdfs;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mazerunner.core.config.ConfigurationLoader;
import org.mazerunner.core.messaging.Sender;
import org.mazerunner.core.models.ProcessorMessage;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Iterator;

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
     * Writes a property graph list as a result of a GraphX algorithm to HDFS.
     * @param processorMessage The path to the HDFS file to be created.
     * @param nodeList The list of node IDs and properties to update in Neo4j.
     * @throws URISyntaxException
     * @throws IOException
     */
    public static void writePropertyGraphUpdate(ProcessorMessage processorMessage, Iterable<String> nodeList) throws URISyntaxException, IOException {
        // Write the nodeList results to HDFS
        int lineCount = writeListFile(processorMessage.getPath(), nodeList.iterator());

        // Make sure there are results to return
        if(lineCount > 0) {
            // Serialize the processor message
            Gson gson = new Gson();
            String message = gson.toJson(processorMessage);

            // Notify Neo4j that a property update list is available for processing
            Sender.sendMessage(message);
        }
    }

    /**
     * Write a file to HDFS line by line.
     * @param path The path to the HDFS file to be created.
     * @param nodeList The list of node IDs and properties to update in Neo4j.
     * @throws IOException
     * @throws URISyntaxException
     */
    public static int writeListFile(String path, Iterator<String> nodeList) throws IOException, URISyntaxException {
        FileSystem fs = getHadoopFileSystem();
        Path updateFilePath = new Path(path);
        BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(updateFilePath,true)));

        br.write("# Node Property Value List\n");
        int lineCount = 0;

        while(nodeList.hasNext()) {
            br.write(nodeList.next());
            lineCount++;
        }

        br.flush();
        br.close();

        return lineCount;
    }

    /**
     * Read the contents of a file and return the results as a string.
     * @param path The path to the HDFS file to be created.
     * @return Returns the full contents of an HDFS file.
     * @throws IOException
     * @throws URISyntaxException
     */
    public static String readHdfsFile(String path) throws IOException, URISyntaxException {
        FileSystem fs = getHadoopFileSystem();
        Path filePath = new Path(path);
        FSDataInputStream inputStream = fs.open(filePath);

        Charset encoding = Charset.defaultCharset();

        byte[] buffer = new byte[inputStream.available()];
        inputStream.readFully(buffer);
        inputStream.close();
        String contents = new String(buffer, encoding);

        return contents;
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
