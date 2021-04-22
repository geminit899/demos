package com.geminit.plugins;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag$;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ColNormalizer {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("zpsb");
        conf.setMaster("local");
        SparkContext sc = new SparkContext(conf);

        String print = "";

        for (int epoch = 0; epoch < 10; epoch++) {
            List<Map<String, String>> list = new ArrayList<>();
            for (int id = 1; id <= 10000; id++) {
                for (int month = 1; month <= 12; month++) {
                    Map<String, String> map = new HashMap<>();
                    map.put("id", String.valueOf(id));
                    map.put("month", String.valueOf(month));
                    map.put("num", new DecimalFormat( "0.00" ).format(Math.random() * 10));
                    list.add(map);
                }
            }

            Seq<Map<String, String>> tmpSeq = JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
            //并行集合，是通过对于驱动程序中的集合调用JavaSparkContext.parallelize来构建的RDD
            JavaRDD<Map<String, String>> input = sc.parallelize(tmpSeq, 10, ClassTag$.MODULE$.apply(Map.class)).toJavaRDD();

            JavaPairRDD<String, Map<String, String>> pairRDD = input.mapToPair(
                    new PairFunction<Map<String, String>, String, Map<String, String>>() {
                        @Override
                        public Tuple2<String, Map<String, String>> call(Map<String, String> record) throws Exception {
                            return new Tuple2(record.get("id"), record);
                        }
                    });

            pairRDD.count();

            long time1 = System.currentTimeMillis();
            JavaRDD<String> groupByRdd = pairRDD.groupByKey().map(new Function<Tuple2<String, Iterable<Map<String, String>>>, String>() {
                @Override
                public String call(Tuple2<String, Iterable<Map<String, String>>> tuple2) throws Exception {
                    String id = tuple2._1();
                    Iterator<Map<String, String>> iterator = tuple2._2().iterator();
                    Map<Integer, Double> map = new HashMap<>();
                    while (iterator.hasNext()) {
                        Map<String, String> record = iterator.next();
                        map.put(Integer.valueOf(record.get("month")), Double.valueOf(record.get("num")));
                    }
                    return "id: " + id + "; map: " + map.toString();
                }
            });
            groupByRdd.count();
            long time2 = System.currentTimeMillis();

            JavaPairRDD<String, Map<Integer, Double>> combinedRdd = pairRDD.combineByKey(new Function<Map<String, String>, Map<Integer, Double>>() {
                @Override
                public Map<Integer, Double> call(Map<String, String> record) throws Exception {
                    Map<Integer, Double> map = new HashMap<>();
                    for (int month = 1; month <= 12; month++) {
                        if (Integer.valueOf(record.get("month")).equals(month)) {
                            map.put(month, Double.valueOf(record.get("num")));
                        } else {
                            map.put(month, 0.0);
                        }
                    }
                    return map;
                }
            }, new Function2<Map<Integer, Double>, Map<String, String>, Map<Integer, Double>>() {
                @Override
                public Map<Integer, Double> call(Map<Integer, Double> map, Map<String, String> record) throws Exception {
                    map.put(Integer.valueOf(record.get("month")), Double.valueOf(record.get("num")));
                    return map;
                }
            }, new Function2<Map<Integer, Double>, Map<Integer, Double>, Map<Integer, Double>>() {
                @Override
                public Map<Integer, Double> call(Map<Integer, Double> o1, Map<Integer, Double> o2) throws Exception {
                    Map<Integer, Double> map = new HashMap<>();
                    for (Map.Entry<Integer, Double> entry : o1.entrySet()) {
                        map.put(entry.getKey(), entry.getValue());
                    }
                    for (Map.Entry<Integer, Double> entry : o2.entrySet()) {
                        map.put(entry.getKey(), entry.getValue());
                    }
                    return map;
                }
            });

            JavaRDD<String> resultRdd = combinedRdd.map(new Function<Tuple2<String, Map<Integer, Double>>, String>() {
                @Override
                public String call(Tuple2<String, Map<Integer, Double>> tuple2) throws Exception {
                    String id = tuple2._1();
                    Map<Integer, Double> map = tuple2._2();
                    return "id: " + id + "; map: " + map.toString();
                }
            });

            resultRdd.count();
            long time3 = System.currentTimeMillis();

            print += "groupBy: " + (time2 - time1) + ", combineBy: " + (time3 - time2) + "\n";
        }
        System.out.println(print);
    }
}
