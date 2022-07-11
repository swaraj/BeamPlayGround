package org.example;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Hello world!
 */
public class GenerateInput {
    static Logger logger = LoggerFactory.getLogger(GenerateInput.class);

    static String topic = "topic";
    static String outputTopic = "output";
    static String groupId = "GROUP_ID";
    static String groupId2 = "GROUP_ID_2";
    static String servers = "localhost:29092,localhost:39092";

    public static void main(String[] args) {
        Properties producerProperties = new Properties();

        producerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, servers);
        producerProperties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "Producer");
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties)) {
            for (int i = 0; i < 100_000; i++) {
                int j = i % 100;
                Future<RecordMetadata> recordMetadataFuture = kafkaProducer.send(new ProducerRecord<>(topic, "key" + j, "value" + j));
                kafkaProducer.flush();
                RecordMetadata recordMetadata = recordMetadataFuture.get(1, TimeUnit.MINUTES);
                logger.warn("Record Metadata {} {} {}", recordMetadata.topic(), recordMetadata.offset(), recordMetadata.partition());
            }
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
