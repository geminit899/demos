package com.geminit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Kafka_Producer {
    private static Map<String, List<String>> messages = new HashMap<>();
    private static String SERVER = "192.168.0.114:6667";
    private static String TOPIC = "FlinkBatchOutput";
    static {
        List<String> htst = new ArrayList<>();
        htst.add("test");
        messages.put("htst", htst);

        List<String> sparkIn = new ArrayList<>();
        sparkIn.add("这，是test1.");
        sparkIn.add("这，是test1.");
        sparkIn.add("这，是test2.");
        messages.put("SparkIn", sparkIn);

        List<String> sparkBatchInput = new ArrayList<>();
        sparkBatchInput.add("这，是test1.");
        sparkBatchInput.add("这，是test1.");
        sparkBatchInput.add("这，是test2.");
        messages.put("SparkBatchInput", sparkBatchInput);

        List<String> SparkBatchOutput = new ArrayList<>();
        SparkBatchOutput.add("小红:18");
        messages.put("SparkBatchOutput", SparkBatchOutput);

        List<String> sparkStreamInput = sparkBatchInput;
        messages.put("SparkStreamInput", sparkStreamInput);

        List<String> flinkBatchInput = new ArrayList<>();
        flinkBatchInput.add("jack:12");
        flinkBatchInput.add("shy:15");
        flinkBatchInput.add("小红:18");
        messages.put("FlinkBatchInput", flinkBatchInput);

        List<String> flinkBatchOutput = new ArrayList<>();
        flinkBatchOutput.add("小红:18");
        messages.put("FlinkBatchOutput", flinkBatchOutput);

        List<String> flinkStreamInput = new ArrayList<>();
        flinkStreamInput.add("jack:12");
        flinkStreamInput.add("jack:15");
        flinkStreamInput.add("shy:17");
//        flinkStreamInput.add("lili:22");
//        flinkStreamInput.add("tom:24");
//        flinkStreamInput.add("mama:28");
//        flinkStreamInput.add("jack:12");
//        flinkStreamInput.add("shy:15");
//        flinkStreamInput.add("小红:18");
        messages.put("FlinkStreamInput", flinkStreamInput);

        List<String> jdbcKafkaInput = new ArrayList<>();
        jdbcKafkaInput.add("这，只是1个test.");
        messages.put("JDBCKafkaInput", jdbcKafkaInput);

        List<String> jdbcJsonInput = new ArrayList<>();
        jdbcJsonInput.add("{}");
        messages.put("JDBCJsonInput", jdbcJsonInput);

        List<String> jsonTest = new ArrayList<>();
        for (int i = 1; i < 6; i++) {
            jsonTest.add("{" + i + "}");
        }
        messages.put("SparkTest", jsonTest);
    }

    public static void main(String[] args) {
        //这个是用来配置kafka的参数
        Properties prop = new Properties();
        //这里不是配置broker.id了，这个是配置bootstrap.servers
        prop.put("bootstrap.servers", SERVER);
        //下面是分别配置 key和value的序列化
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //这个地方和1.0X之前的版本有不一样的，这个是使用kafkaproducer 类来实例化
        Producer<String, String> producer = new KafkaProducer<String, String>(prop);

        List<String> messageList = messages.get(TOPIC);
        for (int i = 0; i < messageList.size(); i++) {
            String message = messageList.get(i);
            //这个send 没有什么变化
            producer.send(new ProducerRecord<String, String>(TOPIC, Integer.toString(i), message));
            System.out.printf("Send key: %s  value: %s.\n", i, message);
        }
//        producer.close();
    }
}
