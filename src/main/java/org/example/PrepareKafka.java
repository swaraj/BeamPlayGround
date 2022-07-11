package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PrepareKafka {
    static Logger logger = LoggerFactory.getLogger(App.class);

    static String topic = "topic";
    static String outputTopic = "output";
    static String groupId = "GROUP_ID";
    static String groupId2 = "GROUP_ID_2";
    static String servers = "localhost:29092,localhost:39092";

    public static void main(String[] args) {
        System.out.println("Preparing Kafka!");

        Properties properties = new Properties();

        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servers);

        try (AdminClient adminClient = AdminClient.create(properties)) {
            List<NewTopic> list = List.of(new NewTopic(topic, 1, (short) 1), new NewTopic(outputTopic, 1, (short) 1));
            adminClient.createTopics(list).all().get(5, TimeUnit.MINUTES);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
