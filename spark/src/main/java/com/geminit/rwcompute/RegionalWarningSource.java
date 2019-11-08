package com.geminit.rwcompute;

import org.apache.commons.lang.time.DateUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class RegionalWarningSource {
    private static final String SPARK_APP_NAME = "StreamingRegionalWarning";
    private static final String POSTGRESQL_DRIVER = "org.postgresql.Driver";
    private static final String POSTGRESQL_USER = "postgres";
    private static final String POSTGRESQL_PASS = "root";
    private static final String POSTGRESQL_URL = "jdbc:postgresql://localhost:5432/test";
    private static final String KAFKA_URL = "localhost:9092";
    private static final String KAFKA_CONSUMER_TOPIC = "rwSourceJob";
    private static final String INSERT_QUERY_SQL = "INSERT INTO rwCompute (indexDate, eventId, dims, spaces, batch, maxNum) VALUES ('%s', '%s', '%s', '%s', %d, %d)";

    private static SimpleDateFormat hmFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private static Properties props = new Properties();
    private static Map<String, Object> kafkaParams = new HashMap<>();

    static {
        try {
            Class.forName(POSTGRESQL_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        props.put("bootstrap.servers", KAFKA_URL);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        kafkaParams.put("bootstrap.servers", KAFKA_URL);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "rwSource");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
    }

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName(SPARK_APP_NAME).setMaster("local[*]");
        sparkConf.set("spark.streaming.backpressure.enabled", "true");
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "10000");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(javaStreamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(Arrays.asList(KAFKA_CONSUMER_TOPIC), kafkaParams));

        JavaDStream<String> javaDStream = stream.map(new Function<ConsumerRecord<String, String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> record) throws Exception {
                return record.value();
            }
        }).repartition(20);
        javaDStream.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD, Time time) throws Exception {

            }
        });
        javaDStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> javaRDD) throws Exception {
                javaRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
                    @Override
                    public void call(Iterator<String> iterator) throws Exception {
                        Connection connection = DriverManager.getConnection(POSTGRESQL_URL, POSTGRESQL_USER, POSTGRESQL_PASS);
                        while (iterator.hasNext()) {
                            //2019-01-02 17:45:00__2019-01-02 17:50:00__001__imsi,face__1,2__5__1000
                            String[] kafka = iterator.next().split("__");
                            Date startTime = hmFormat.parse(kafka[0]);
                            Date endTime = hmFormat.parse(kafka[1]);
                            String eventId = kafka[2];
                            String dims = kafka[3];
                            String spaces = kafka[4];
                            int batch = Integer.parseInt(kafka[5]);
                            int maxNum = Integer.parseInt(kafka[6]);
                            Date indexTime = new Date();

                            int count = countMinutes(startTime, endTime, indexTime);
                            for (int i = 0; i < count; i++) {
                                String sql = String.format(INSERT_QUERY_SQL, indexTime, eventId, dims, spaces, batch, maxNum);
                                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                                preparedStatement.execute();
                                indexTime = DateUtils.addMinutes(indexTime, 1);
                            }
                        }
                        connection.close();
                    }
                });
            }
        });
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }

    public static int countMinutes(Date startTime, Date endTime, Date currentTime) {
        //计算一共多少分钟，决定执行多少次
        int count;
        if (currentTime.after(startTime)) {
            startTime = currentTime;
        }
        if (currentTime.after(endTime)) {
            count = 0;
        } else {
            long dateDiff = endTime.getTime() - startTime.getTime();
            long nd = 1000 * 24 * 60 * 60;
            long nh = 1000 * 60 * 60;
            long nm = 1000 * 60;
            long minDiff = dateDiff % nd % nh / nm;
            count = (int) minDiff + 1;
        }
        return count;
    }
}
