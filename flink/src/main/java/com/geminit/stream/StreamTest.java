package com.geminit.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public class StreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
//        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "test");

//        FlinkKafkaConsumer08<String> myConsumer = new FlinkKafkaConsumer08<String>("inputTest", new SimpleStringSchema(),properties);

        FlinkKafkaConsumer010<String> consumer010 = new FlinkKafkaConsumer010<String>("inputTest", new SimpleStringSchema(),properties);
        FlinkKafkaConsumerBase<String> flinkKafkaConsumerBase = consumer010.setStartFromLatest();;

        DataStream<String> stream = see.addSource(flinkKafkaConsumerBase);

        DataStream<String> out = stream.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return "The kafka content is ------- " + s;
            }
        });

        out.print();

//        AllWindowedStream allWindowedStream = stream.timeWindowAll(Time.seconds(1));
//
//        DataStream<String> out = stream.timeWindowAll(Time.seconds(1)).apply(new AllWindowFunction<String, String, TimeWindow>() {
//            @Override
//            public void apply(TimeWindow timeWindow, Iterable<String> iterable, Collector<String> collector) {
//                StreamExecutionEnvironment inSee = StreamExecutionEnvironment.getExecutionEnvironment();
//                DataStream<String> dataStream = inSee.fromCollection(Lists.newArrayList(iterable));
//
//                DataStream<String> test = dataStream.map(new MapFunction<String, String>() {
//                    @Override
//                    public String map(String s) {
//                        return s;
//                    }
//                });
//
//                try {
//                    Iterator<String> list = DataStreamUtils.collect(test);
//                    while (list.hasNext()) {
//                        collector.collect(list.next());
//                    }
//
//                    inSee.execute();
//                } catch (Exception e) {
//
//                }
//            }
//        });
//        out.print();
//        DataStream<String> countOut = out.countWindowAll(3).apply(new AllWindowFunction<String, String, GlobalWindow>() {
//            @Override
//            public void apply(GlobalWindow globalWindow, Iterable<String> iterable, Collector<String> collector) throws Exception {
//                StreamExecutionEnvironment inSee = StreamExecutionEnvironment.getExecutionEnvironment();
//                DataStream<String> dataStream = inSee.fromCollection(Lists.newArrayList(iterable));
//
//                DataStream<String> test = dataStream.map(new MapFunction<String, String>() {
//                    @Override
//                    public String map(String s) {
//                        return "count - " + s;
//                    }
//                });
//
//                try {
//                    Iterator<String> list = DataStreamUtils.collect(test);
//                    while (list.hasNext()) {
//                        collector.collect(list.next());
//                    }
//
//                    inSee.execute();
//                } catch (Exception e) {
//                    e.getLocalizedMessage();
//                }
//            }
//        });
//        countOut.print();
//        DataStream<String> timeOut = countOut.timeWindowAll(Time.seconds(1)).apply(new AllWindowFunction<String, String, TimeWindow>() {
//            @Override
//            public void apply(TimeWindow timeWindow, Iterable<String> iterable, Collector<String> collector) throws Exception {
//                StreamExecutionEnvironment inSee = StreamExecutionEnvironment.getExecutionEnvironment();
//                DataStream<String> dataStream = inSee.fromCollection(Lists.newArrayList(iterable));
//
//                DataStream<String> test = dataStream.map(new MapFunction<String, String>() {
//                    @Override
//                    public String map(String s) {
//                        return "time - " + s;
//                    }
//                });
//
//                try {
//                    Iterator<String> list = DataStreamUtils.collect(test);
//                    while (list.hasNext()) {
//                        collector.collect(list.next());
//                    }
//
//                    inSee.execute();
//                } catch (Exception e) {
//                    e.getCause();
//                }
//            }
//        });
//        timeOut.print();

        see.execute();
    }
}
