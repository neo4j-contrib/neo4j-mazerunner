package org.mazerunner.core.messaging;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.mazerunner.core.config.ConfigurationLoader;
import org.mazerunner.core.models.ProcessorMessage;
import org.mazerunner.core.processor.GraphProcessor;

import java.util.ArrayList;
import java.util.List;

import static org.kohsuke.args4j.ExampleMode.ALL;

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

public class Worker {

    private static final String TASK_QUEUE_NAME = "jobs";

    @Option(name="--spark.app.name",usage="The Spark application name (e.g. mazerunner).",metaVar="<string>")
    private String sparkAppName = "mazerunner";

    @Option(name="--spark.executor.memory",usage="Amount of memory to use per executor process, in the same format as JVM memory strings (e.g. 512m, 2g). ", metaVar = "<string>")
    private String sparkExecutorMemory = "4092m";

    @Option(name="--spark.master",usage="The Spark master URL (e.g. spark://localhost:7077).",metaVar="<url>")
    private String sparkMaster = "local[8]";

    @Option(name="--hadoop.hdfs",usage="The HDFS URL (e.g. hdfs://0.0.0.0:9000).", metaVar = "<url>")
    private String hadoopHdfs = "hdfs://0.0.0.0:9000";

    @Option(name="--spark.driver.host",usage="The host name of the Spark driver (eg. ec2-54-67-91-4.us-west-1.compute.amazonaws.com)", metaVar = "<url>")
    private String driverHost = "mazerunner";

    @Option(name="--rabbitmq.host",usage="The host name of the rabbitmq server.", metaVar = "<url>")
    private String rabbitMqHost = "localhost";

    // receives other command line parameters than options
    @Argument
    private List<String> arguments = new ArrayList<String>();

    public Worker() {

    }

    public static void main(String[] args) throws Exception {
        new Worker().doMain(args);
    }

    public void doMain(String[] args) throws Exception {

        CmdLineParser parser = new CmdLineParser(this);

        // if you have a wider console, you could increase the value;
        // here 80 is also the default
        parser.setUsageWidth(80);

        try {
            // parse the arguments.
            parser.parseArgument(args);

            if(sparkMaster == "" || hadoopHdfs == "")
                throw new CmdLineException(parser, "Options required: --hadoop.hdfs <url>, --spark.master <url>");

            ConfigurationLoader.getInstance().setHadoopHdfsUri(hadoopHdfs);
            ConfigurationLoader.getInstance().setSparkHost(sparkMaster);
            ConfigurationLoader.getInstance().setAppName(sparkAppName);
            ConfigurationLoader.getInstance().setExecutorMemory(sparkExecutorMemory);
            ConfigurationLoader.getInstance().setDriverHost(driverHost);
            ConfigurationLoader.getInstance().setRabbitmqNodename(rabbitMqHost);

        } catch( CmdLineException e ) {
            // if there's a problem in the command line,
            // you'll get this exception. this will report
            // an error message.
            System.err.println(e.getMessage());
            System.err.println("java -cp $CLASSPATH [<spark-config-options>] <main-class> [<mazerunner-args>]");
            // print the list of available options
            parser.printUsage(System.err);
            System.err.println();

            // print option sample. This is useful some time
            System.err.println("  Example: java -cp $CLASSPATH org.mazerunner.core.messaging.Worker"+parser.printExample(ALL));

            return;
        }

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(ConfigurationLoader.getInstance().getRabbitmqNodename());
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

        channel.basicQos(20);

        // Initialize spark context
        GraphProcessor.initializeSparkContext();

        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(TASK_QUEUE_NAME, false, consumer);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());

            System.out.println(" [x] Received '" + message + "'");

            // Deserialize message
            Gson gson = new Gson();
            ProcessorMessage processorMessage = gson.fromJson(message, ProcessorMessage.class);

            // Run PageRank
            GraphProcessor.processEdgeList(processorMessage);

            System.out.println(" [x] Done '" + message + "'");
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }
    }
}


