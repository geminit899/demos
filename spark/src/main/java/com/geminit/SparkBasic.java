package com.geminit;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag$;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SparkBasic {
    public static SparkContext SC;
    public static JavaStreamingContext JSC;

    static {
        SparkConf conf = new SparkConf();
        conf.setAppName("zpsb");
        conf.setMaster("local");
//        conf.set("", "");
        SC = new SparkContext(conf);
    }

    public static void main(String[] args) {
        List<Object> list = new ArrayList<>();
        Obj obj = new Obj("sss");
        list.add(obj);
        list.add(obj);
        list.add(new Obj("imsi"));
        list.add(new Obj("imsi"));
        JavaRDD<Object> javaRDD = listToRdd(list);
        JavaPairRDD<Object, String> pairRDD = javaRDD.mapToPair(new PairFunction<Object, Object, String>() {
            @Override
            public Tuple2<Object, String> call(Object object) throws Exception {
                return new Tuple2<>(object, ((Obj) object).getName());
            }
        });
        JavaPairRDD<Object, String> flatMapRDD = pairRDD.groupByKey().mapToPair(
                new PairFunction<Tuple2<Object, Iterable<String>>, Object, String>() {
            @Override
            public Tuple2<Object, String> call(Tuple2<Object, Iterable<String>> tuple2) throws Exception {
                String value = "";
                Iterator<String> interator = tuple2._2.iterator();
                while (interator.hasNext()) {
                    String s = interator.next();
                    value += s + ";";
                }
                return new Tuple2<>(tuple2._1, value);
            }
        });
        System.out.println(flatMapRDD.collect().toString());
    }

    public static JavaRDD listToRdd(List<Object> list) {
        Seq<Object> tmpSeq = JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
        //并行集合，是通过对于驱动程序中的集合调用JavaSparkContext.parallelize来构建的RDD
        RDD<Object> rdd = SC.parallelize(tmpSeq, 10, ClassTag$.MODULE$.apply(String.class));
        JavaRDD<Object> javaRDD = rdd.toJavaRDD();
        return javaRDD;
    }

    public static class Obj implements Serializable {
        private String name;

        public Obj(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }
    }
}
