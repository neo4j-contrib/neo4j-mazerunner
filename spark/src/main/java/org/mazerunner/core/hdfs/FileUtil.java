package org.mazerunner.core.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mazerunner.core.messaging.Sender;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

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
     * @param path The path to the HDFS file to be created.
     * @param nodeList The list of node IDs and properties to update in Neo4j.
     * @throws URISyntaxException
     * @throws IOException
     */
    public static void writePropertyGraphUpdate(String path, String nodeList) throws URISyntaxException, IOException {
        FileSystem fs = getHadoopFileSystem();
        Path updateFilePath = new Path(path);
        BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(updateFilePath,true)));

        br.write("# Node Property Value List");
        BufferedReader nodeListReader = new BufferedReader(new StringReader(nodeList));

        String line;
        while((line = nodeListReader.readLine()) != null) {
            br.write( "\n" + line);
        }
        br.flush();
        br.close();

        // Notify Neo4j that a property update list is available for processing
        Sender.sendMessage(path);
    }

    public static String readGraphAdjacenyList(String path) throws IOException, URISyntaxException {
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
    private static FileSystem getHadoopFileSystem() throws IOException, URISyntaxException {
        /* TODO
        * Extract configurations into a property file */
        Configuration hadoopConfiguration = new Configuration();
        hadoopConfiguration.addResource(new Path("/Users/kennybastani/Downloads/hadoop-2.4.1/etc/hadoop/core-site.xml"));
        hadoopConfiguration.addResource(new Path("/Users/kennybastani/Downloads/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
        hadoopConfiguration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        return FileSystem.get(new URI("hdfs://0.0.0.0:9000"), hadoopConfiguration);
    }
}
