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
public class ReadOutput {
    static Logger logger = LoggerFactory.getLogger(ReadOutput.class);

    static String topic = "topic";
    static String outputTopic = "output";
    static String groupId = "GROUP_ID";
    static String groupId2 = "GROUP_ID_2";
    static String servers = "localhost:29092,localhost:39092";

    public static void main(String[] args) {
        Properties consumerProperties2 = new Properties();

        consumerProperties2.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        consumerProperties2.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties2.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties2.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId2);
        consumerProperties2.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProperties2.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");
        consumerProperties2.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties2)) {
            kafkaConsumer.subscribe(Collections.singleton(outputTopic));

            ConsumerRecords<String, String> ssCRs = kafkaConsumer.poll(Duration.ofMinutes(1));

            logger.warn("Consumer records :: {}", ssCRs.count());

            for (ConsumerRecord<String, String> ssCR : ssCRs) {
                logger.warn("Found Data {} {} {} {}", ssCR.offset(), ssCR.partition(), ssCR.key(), ssCR.value());
            }

            kafkaConsumer.unsubscribe();
        }
    }
}
