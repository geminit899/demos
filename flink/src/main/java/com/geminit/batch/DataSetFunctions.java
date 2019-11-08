package com.geminit.batch;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class DataSetFunctions {
    public static void main(String[] args) {
        DataSet join1 = EEBasic.getDataSetPairFromElements(new Tuple2<Object, Object>("TEST", "tyx"));
        DataSet join2 = EEBasic.getDataSetPairFromElements(new Tuple2<Object, Object>("TEST", 22));


    }

    public static DataSet mapFuntion(DataSet dataSet) {
        DataSet<Tuple2<String, String>> test = dataSet.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, String>>() {
			@Override
			public Tuple2<String, String> map(Tuple2<String, Integer> tuple2) throws Exception {
				return null;
			}
		});
        return test;
    }

    public static DataSet flatMapFunction(DataSet dataSet) {
        DataSet<Tuple2<String, String>> test = dataSet.flatMap(new FlatMapFunction<Tuple2<String, Integer>, Object>() {
			@Override
			public void flatMap(Tuple2<String, Integer> tuple2, Collector<Object> collector) {
				collector.collect(tuple2);
			}
		});
        return test;
    }

    public static DataSet groupByFunction(DataSet dataSet) {
		DataSet<Tuple2<String, Integer>> test = dataSet.groupBy(0)
				.reduceGroup(new GroupReduceFunction<Tuple2<String,Integer>, Tuple2<String, Integer>>() {
			@Override
			public void reduce(Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) {
				Iterator<Tuple2<String, Integer>> it = iterable.iterator();
				while (it.hasNext()) {
					Tuple2<String, Integer> tuple2 = it.next();
					String ss;
					if (tuple2.f0.equals("lt")) {
						ss = "0";
					} else {
						ss = "1";
					}
					collector.collect(new Tuple2<String, Integer>(ss, tuple2.f1));
				}
			}
		});
		return test;
    }

    public static DataSet joinFunction(DataSet join1, DataSet join2) {
        DataSet<Tuple2<String, Integer>> test1 = join1.leftOuterJoin(join2).where(new KeySelector<Tuple2<String,String>, Object>() {
			@Override
			public Object getKey(Tuple2<String, String> tuple2) {
				return null;
			}
		}).equalTo(new KeySelector<Tuple2<String,Integer>, Object>() {
			@Override
			public Object getKey(Tuple2<String, Integer> tuple2) {
				return null;
			}
		}).with(new JoinFunction<Tuple2<String, String>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> join(Tuple2<String, String> tuple2, Tuple2<String, Integer> tuple22) throws Exception {
				return null;
			}
		});

		DataSet<Tuple2<String, Integer>> test2 = join1.leftOuterJoin(join2).where(0).equalTo(0)
				.with(new JoinFunction<Tuple2<String, String>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> join(Tuple2<String, String> t1, Tuple2<String, Integer> t2)
					throws Exception {
				return new Tuple2<>(t1.f1, t2.f1);
			}
		});
		return test1;
    }

    public static DataSet filterFunction(DataSet dataSet) {
        DataSet<Integer> test = dataSet.filter(new FilterFunction<Integer>() {
			@Override
			public boolean filter(Integer integer) throws Exception {
				return integer % 2 == 0;
			}
		});
        return test;
    }
}
