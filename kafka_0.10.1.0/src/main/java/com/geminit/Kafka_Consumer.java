package com.geminit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class Kafka_Consumer {
    private static String SERVER = "192.168.0.114:6667";
    private static String TOPIC = "FlinkBatchOutput";

    public static void main(String [] args){
        //这个是用来配置kafka的参数
        Properties props=new Properties();
        //这里不是配置zookeeper了，这个是配置bootstrap.servers
        props.put("bootstrap.servers", SERVER);
        //这个配置组，之前我记得好像不配置可以，现在如果不配置那么不能运行
        props.put("group.id", "test");
        props.put("auto.offset.reset", "earliest");
        //序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        Consumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        //配置topic
        consumer.subscribe(Arrays.asList(TOPIC));
        while (true) {
            //这里是得到ConsumerRecords实例
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                //这个有点好，可以直接通过record.offset()得到offset的值
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }
}
