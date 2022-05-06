package com.selflearning;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Cluster;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class Producer {
    public static void main(String[] args) throws Exception {
        Properties pros=new Properties();
        pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        pros.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.selflearning.Partitioner");
        pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        // 0 发出去就确认 | 1 leader 落盘就确认| all(-1) 所有Follower同步完才确认
        pros.put(ProducerConfig.ACKS_CONFIG,"-1");
        // 异常自动重试次数
        pros.put(ProducerConfig.RETRIES_CONFIG,3);
        // 多少条数据发送一次，默认16K
        pros.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        // 批量发送的等待时间
        pros.put(ProducerConfig.LINGER_MS_CONFIG,5000);
        // 客户端缓冲区大小，默认32M，满了也会触发消息发送
        pros.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        // 获取元数据时生产者的阻塞时间，超时后抛出异常
        pros.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,3000);
        pros.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test");
        pros.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 5000);

        // 创建Sender线程
        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(pros);
        producer.initTransactions();
        producer.beginTransaction();
        for (int i =0 ;;i++) {
            producer.send(new ProducerRecord<String, String>("mytopic", Integer.toString(i), Integer.toString(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e!=null) System.out.println(e);
                }
            });
            System.out.println("发送:"+i);
        }
//        producer.commitTransaction();
//        System.in.read();
//        producer.close();
    }
}
