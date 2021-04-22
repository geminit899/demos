package com.geminit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Kafka_Producer_demo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("metadata.broker.list", "192.168.0.114:6667");
        props.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
        props.setProperty("key.serializer.class", "kafka.serializer.StringEncoder");
        props.setProperty("request.required.acks", "1");
        props.setProperty("producer.type", "sync");
        props.setProperty("queue.buffering.max.ms", "1000");
        props.setProperty("log.publish.num.partitions", "10");

        ProducerConfig config = new ProducerConfig(props);
        Producer producer = new Producer<>(config);

        List<KeyedMessage<String, byte[]>> messages = new ArrayList<>();
        messages.add(new KeyedMessage<>("logs.user-v2", "system:services:appfabric", "123".getBytes()));

        producer.send(messages);
        System.out.println("Send.");
        producer.close();
    }
}
