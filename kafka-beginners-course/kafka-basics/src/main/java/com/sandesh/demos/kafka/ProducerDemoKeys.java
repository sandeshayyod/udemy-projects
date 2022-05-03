package com.sandesh.demos.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerDemoKeys {
    public static void main(String[] args) {
        log.info("I am a Kafka Producer With Keys");

        // configure kafka properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create a producer
        Producer<String, String> producer = new KafkaProducer<>(properties);


        for (int i = 10; i < 15; i++) {

            String topic = "demo_java";
            String key = "id"+i;
            String value = "hello world" + i;

            // create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            // send the record to kafka via producer
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    //successfully sent to topic
                    log.info("Received new metadata. /\n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "key: " + producerRecord.key() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp());
                } else {
                    log.error("Error while producing", exception);
                }
            });
        }


        // flush and close the producer
        producer.flush();
        producer.close();
    }
}
