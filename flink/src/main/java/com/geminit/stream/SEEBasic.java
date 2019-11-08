package com.geminit.stream;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SEEBasic {
    public static StreamExecutionEnvironment SEE = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.0.114:6667");
        properties.setProperty("zookeeper.connect", "192.168.0.114:2181");
        properties.setProperty("group.id", "tests");
        FlinkKafkaConsumer010<String> flinkKafkaConsumer010 =
                new FlinkKafkaConsumer010<>("JsonTest", new SimpleStringSchema(), properties);
        FlinkKafkaConsumerBase<String> flinkKafkaConsumerBase = flinkKafkaConsumer010.setStartFromLatest();

        DataStream<String> dataStream = SEE.addSource(flinkKafkaConsumerBase);

        dataStream.flatMap(new FlatMapFunction<String, StructuredRecord>() {
            @Override
            public void flatMap(String s, Collector<StructuredRecord> collector) throws Exception {
                collector.collect(new StructuredRecord(s));
            }
        }).countWindowAll(2).apply(new AllWindowFunction<StructuredRecord, StructuredRecord, GlobalWindow>() {
            @Override
            public void apply(GlobalWindow globalWindow, Iterable<StructuredRecord> iterable, Collector<StructuredRecord> collector) throws Exception {
                String res = "";
                for (StructuredRecord sr : iterable) {
                    res += sr.getField().getValue() + "; ";
                }
                collector.collect(new StructuredRecord("~~~" + res + "~~~"));
            }
        }).map(new MapFunction<StructuredRecord, String>() {
            @Override
            public String map(StructuredRecord sr) throws Exception {
                return sr.getField().getValue();
            }
        }).print();

        SEE.execute();
    }

    public static ExecutionConfig getConfig() {
        return SEE.getConfig();
    }

    public static DataStream getDataStreamFromSource() {
        return SEE.addSource(new WikipediaEditsSource());
    }

    public static DataStream getDataStreamFromElements(Object ...objects) {
        return SEE.fromElements(objects);
    }

    public static DataStream getDataStreamPairFromElements(Tuple2<Object, Object> ...tuple2s) {
        return SEE.fromElements(tuple2s);
    }
}

class StructuredRecord {
    private Type type;
    private Field field;

    StructuredRecord(String value) {
        this.type = new Type("test");
        this.field = new Field("message", value);
    }

    public Type getType() {
        return this.type;
    }

    public Field getField() {
        return this.field;
    }

    class Type {
        String type;

        public Type(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }
    }

    class Field {
        private String name;
        private String value;

        private Field(String name, String value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public String getValue() {
            return value;
        }
    }
}