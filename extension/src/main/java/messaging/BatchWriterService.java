package messaging;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import config.ConfigurationLoader;
import hdfs.FileUtil;
import models.ProcessorMessage;
import org.neo4j.graphdb.GraphDatabaseService;
import translation.Writer;

import java.io.BufferedReader;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class BatchWriterService extends AbstractScheduledService {

    private static final Logger logger = Logger.getLogger(BatchWriterService.class.getName());
    private GraphDatabaseService graphDb;

    public void SetGraphDatabase(GraphDatabaseService graphDb){
        this.graphDb = graphDb;
    }

    public final static BatchWriterService INSTANCE = new BatchWriterService();
    private BatchWriterService() {
        if (!this.isRunning()){
            logger.info("Starting BatchWriterService");
            this.startAsync();
            this.awaitRunning();
            logger.info("Started BatchWriterService");
        }
    }

    private static final String EXCHANGE_NAME = "processor";

    @Override
    protected void runOneIteration() throws Exception {
        logger.info("Connecting to RabbitMQ processor queue...");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(ConfigurationLoader.getInstance().getRabbitmqNodename());
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.println(" [*] Waiting for processor messages.");

        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(queueName, true, consumer);

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());

            System.out.println(" [x] Received processor message '" + message + "'");

            // Deserialize the processor message
            Gson gson = new Gson();
            ProcessorMessage processorMessage = gson.fromJson(message, ProcessorMessage.class);

            // Open the node property update list file from HDFS
            BufferedReader bufferedReader = FileUtil.readGraphAdjacencyList(processorMessage);

            // Stream the the updates as parallel transactions to Neo4j
            Writer.asyncUpdate(bufferedReader, graphDb, processorMessage.getAnalysis());

            // Close the stream
            bufferedReader.close();
        }
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(0, 1000000, TimeUnit.SECONDS);
    }

}