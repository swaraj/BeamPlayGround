package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Hello world!
 */
public class ReadInput {
    static Logger logger = LoggerFactory.getLogger(ReadInput.class);

    static String topic = "topic";
    static String outputTopic = "output";
    static String groupId = "GROUP_ID";
    static String groupId2 = "GROUP_ID_2";
    static String servers = "localhost:29092,localhost:39092";

    public static void main(String[] args) {
        Properties consumerProperties = new Properties();

        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProperties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties)) {
            kafkaConsumer.subscribe(Collections.singleton(topic));

            ConsumerRecords<String, String> ssCRs = kafkaConsumer.poll(Duration.ofMinutes(1));

            logger.info("Consumer records :: {}", ssCRs.count());

            for (ConsumerRecord<String, String> ssCR : ssCRs) {
                logger.info("Found Data {} {} {} {}", ssCR.offset(), ssCR.partition(), ssCR.key(), ssCR.value());
            }

            kafkaConsumer.unsubscribe();
        }
    }
}
