package messaging;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import config.ConfigurationLoader;
import hdfs.FileUtil;
import jobs.PartitionedAnalysis;
import models.JobRequestType;
import models.ProcessorMessage;
import org.neo4j.graphdb.GraphDatabaseService;
import translation.Writer;

import java.io.BufferedReader;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class BatchWriterService extends AbstractScheduledService {

    private static final Logger logger = Logger.getLogger(BatchWriterService.class.getName());
    private GraphDatabaseService graphDb;

    public void SetGraphDatabase(GraphDatabaseService graphDb){
        this.graphDb = graphDb;
    }

    public static BatchWriterService INSTANCE = new BatchWriterService();

    private BatchWriterService() {
        if (!this.isRunning()){
            logger.info("Starting BatchWriterService");
            this.startAsync();
            this.awaitRunning();
            logger.info("Started BatchWriterService");
        }
    }

    private static final String TASK_QUEUE_NAME = "processor";

    @Override
    protected void runOneIteration() throws Exception {
        logger.info("Connecting to RabbitMQ processor queue...");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(ConfigurationLoader.getInstance().getRabbitmqNodename());
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        channel.basicQos(1);

        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(TASK_QUEUE_NAME, false, consumer);

        while (true) {
            QueueingConsumer.Delivery delivery = null;
            try {
                delivery = consumer.nextDelivery(40000L);

                if(delivery != null) {
                    String message = new String(delivery.getBody());

                    System.out.println(" [x] Received processor message '" + message + "'");

                    // Deserialize the processor message
                    Gson gson = new Gson();
                    ProcessorMessage processorMessage = gson.fromJson(message, ProcessorMessage.class);

                    // Open the node property update list file from HDFS
                    BufferedReader bufferedReader = FileUtil.readGraphAdjacencyList(processorMessage);

                    switch (processorMessage.getMode()) {
                        case Partitioned:
                            PartitionedAnalysis.updatePartition(processorMessage, bufferedReader, graphDb);
                            break;
                        case Unpartitioned:
                            if (Objects.equals(processorMessage.getAnalysis(), JobRequestType.COLLABORATIVE_FILTERING.toString().toLowerCase())) {
                                Writer.asyncImportCollaborativeFiltering(bufferedReader, graphDb);
                            } else {
                                // Stream the the updates as parallel transactions to Neo4j
                                Writer.asyncUpdate(processorMessage, bufferedReader, graphDb);
                            }
                            break;
                    }

                    // Close the stream
                    bufferedReader.close();

                    System.out.println(" [x] Done");

                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                System.out.println("Waiting...");

                // Hold on error cycle to prevent high throughput writes to console log
                Thread.sleep(5000);
                System.out.println("Recovered...");

                if(delivery != null)
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        }
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(0, 1000000, TimeUnit.SECONDS);
    }

}