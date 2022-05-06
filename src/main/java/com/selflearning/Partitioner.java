package com.selflearning;

import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author zhouhao
 * @date 2022/3/30
 */
public class Partitioner implements org.apache.kafka.clients.producer.Partitioner{

    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        int count=cluster.partitionCountForTopic(s);
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}