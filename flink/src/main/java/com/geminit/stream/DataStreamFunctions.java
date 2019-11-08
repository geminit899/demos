package com.geminit.stream;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;

public class DataStreamFunctions {
    public static void main(String[] args) {

    }

    public static DataStream mapFunction(DataStream dataStream) {
        DataStream<Tuple2<String, Long>> ss = dataStream.map(new MapFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(WikipediaEditEvent wikipediaEditEvent) throws Exception {
                return new Tuple2<>(wikipediaEditEvent.getUser(), wikipediaEditEvent.getTimestamp());
            }
        });
        return ss;
    }

    //得到一个带有时间标记的数据流
    public static DataStream getDataStreamWithTime(DataStream dataStream) {
        DataStream<WikipediaEditEvent> dataWithTime = dataStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<WikipediaEditEvent>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(WikipediaEditEvent wikipediaEditEvent) {
                return wikipediaEditEvent.getTimestamp();
            }
        });
        return dataWithTime;
    }

    //对商品进行分组
    public static DataStream keyByFunction(DataStream dataStream) {
        KeyedStream<WikipediaEditEvent, String> keyedEdits = dataStream.keyBy(new KeySelector<WikipediaEditEvent, String>() {
            @Override
            public String getKey(WikipediaEditEvent event) {
                return event.getUser();
            }
        });
        return keyedEdits;
    }

    public static DataStream countWindowFunction(KeyedStream keyedEdits) {
        //使用.timeWindow(Time size, Time slide)对每个商品做滑动窗口
        DataStream<Tuple2<String, Long>> result = keyedEdits.countWindow(2)
                .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {

                acc.f0 = event.getUser();
                acc.f1 += 1;
                return acc;
            }
        });
        return result;
    }
}
