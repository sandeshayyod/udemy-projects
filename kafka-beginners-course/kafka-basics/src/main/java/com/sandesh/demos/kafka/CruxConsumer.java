package com.sandesh.demos.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class CruxConsumer {

    private static Long latestOffSet = 0L;

    public static void main(String[] args) {

        String topic = "first_topic";
        KafkaConsumer<String, String> consumer = createConsumer(topic);
        pollAndPrint(consumer);

        System.out.println("latestOffSet = "+latestOffSet);

        KafkaConsumer<String, String> consumer1 = createConsumer(topic);
        pollAndPrint(consumer1);
    }

    private static void pollAndPrint(KafkaConsumer<String, String> consumer) {
        System.out.println("Polling");
        ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(10000));
        for (ConsumerRecord<String, String> record : records) {
            System.out.println("Key: " + record.key() + ", Value: " + record.value());
            System.out.println("Partition: " + record.partition() + ", Offset: " + record.offset());
            latestOffSet = record.offset();
        }
        consumer.close();
    }

    private static KafkaConsumer<String, String> createConsumer(String topic) {
        // configure kafka properties
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                //commit
                System.out.println("onPartitionsRevoked");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition: partitions) {
                    consumer.seek(partition, latestOffSet);
                }
            }
        });
        return consumer;
    }
}
