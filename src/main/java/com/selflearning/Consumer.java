package com.selflearning;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put("group.id", "test0");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
//        props.put("auto.commit.interval.ms", "1000");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("mytopic"));

        while (true) {
            ConsumerRecords<String, String> mes = consumer.poll(Duration.ofSeconds(1));
            if (mes.count() == 0) continue;
            System.out.println("begin");
            for (ConsumerRecord<String, String> item : mes) {
                System.out.println(item.key() + ":" + item.value());
            }
            consumer.commitSync();
        }
    }
}
