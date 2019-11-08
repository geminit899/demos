package com.geminit.batch;

import com.geminit.FlinkOutputFormat;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BatchTest {
    public static void main(String[] args) throws Exception {
        List<Tuple2<Object, Object>> list = new ArrayList<>();
        list.add(new Tuple2<>(new Obj("Imsi"), new Obj("110")));
        list.add(new Tuple2<>(new Obj("Imsi"), new Obj("220")));
        DataSet<Tuple2<Object, Object>> sinkDataSet = EEBasic.EE.fromCollection(list);

        DataSet<Tuple2<String, Integer>> test = sinkDataSet.groupBy(new KeySelector<Tuple2<Object,Object>, String>() {
            @Override
            public String getKey(Tuple2<Object, Object> tuple2) {
                return ((Obj) tuple2.f0).getName();
            }
        }).reduceGroup(new GroupReduceFunction<Tuple2<Object, Object>, Tuple2<String, Integer>>() {
            @Override
            public void reduce(Iterable<Tuple2<Object, Object>> iterable, Collector<Tuple2<String, Integer>> collector) {
                Iterator<Tuple2<Object, Object>> it = iterable.iterator();
                while (it.hasNext()) {
                    Tuple2<Object, Object> tuple2 = it.next();
                    String ss;
                    if (tuple2.f0.equals("lt")) {
                        ss = "0";
                    } else {
                        ss = "1";
                    }
                    collector.collect(new Tuple2<String, Integer>(ss + "1", Integer.parseInt(ss) + 1));
                }
            }
        });
        test.print();
    }

    public static class Obj {
        private String name;

        public Obj(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }
    }
}
