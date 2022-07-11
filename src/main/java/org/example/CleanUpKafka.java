package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class CleanUpKafka {
    static Logger logger = LoggerFactory.getLogger(App.class);

    static String topic = "topic";
    static String outputTopic = "output";
    static String groupId = "GROUP_ID";
    static String groupId2 = "GROUP_ID_2";
    static String servers = "localhost:29092,localhost:39092";

    public static void main(String[] args) {
        System.out.println("Cleaning Up Kafka!");

        Properties properties = new Properties();

        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servers);

        try (AdminClient adminClient = AdminClient.create(properties)) {

            List<String> groupList = List.of(groupId, groupId2);

            Map<String, ConsumerGroupDescription> cgMap = adminClient.describeConsumerGroups(groupList).all().get(1, TimeUnit.MINUTES);
            adminClient.deleteConsumerGroups(cgMap.values().stream().map(ConsumerGroupDescription::groupId).collect(Collectors.toList())).all().get(1, TimeUnit.MINUTES);
            logger.info("Deleted ConsumerGroups {}", cgMap.values());

            Set<String> topics = adminClient.listTopics().names().get(1, TimeUnit.MINUTES);
            logger.info("Found Topics {}", topics);

            adminClient.deleteTopics(topics).all().get(1, TimeUnit.MINUTES);
            logger.info("Deleted Topics {}", topics);

        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
