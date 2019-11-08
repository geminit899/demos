package com.geminit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SparkStreaming {
    public static SparkContext SC;
    public static JavaStreamingContext JSC;

    static {
        SparkConf conf = new SparkConf();
        conf.setAppName("zpsb");
        conf.setMaster("local[*]");
//        SC = new SparkContext(conf);
        JSC = new JavaStreamingContext(conf, Durations.seconds(1));
    }

    public static final String KAFKA_URL = "192.168.0.114:6667";
    public static final String KAFKA_CONSUMER_TOPIC = "SparkStreamSource";
    private static Map<String, Object> kafkaParams = new HashMap<>();
    static {
        kafkaParams.put("bootstrap.servers", KAFKA_URL);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "rwSource");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
    }

    public static void main(String[] args) throws Exception {
        JavaDStream javaDStream = getInput();

        javaDStream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaRDD<String> o) throws Exception {
                return o.map(new Function<String, String>() {
                    @Override
                    public String call(String s) {
                        return s + "_test";
                    }
                });
            }
        }).print();

        JSC.start();
        JSC.awaitTermination();
    }

    public static JavaDStream getInput() {
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(JSC,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(Arrays.asList(KAFKA_CONSUMER_TOPIC), kafkaParams));
        JavaDStream<String> javaDStream = stream.map(new Function<ConsumerRecord<String, String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> record) throws Exception {
                return record.value();
            }
        }).repartition(20);
        return javaDStream;
    }
}
