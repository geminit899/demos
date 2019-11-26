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
        List<String> list = new ArrayList<>();
        list.add("car");
        list.add("imsi");
        list.add("face");
        Seq<String> tmpSeq = JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
        //并行集合，是通过对于驱动程序中的集合调用JavaSparkContext.parallelize来构建的RDD
        JavaRDD<String> javaRDD = SC.parallelize(tmpSeq, 2, ClassTag$.MODULE$.apply(String.class)).toJavaRDD();
        JavaPairRDD<String, String> pairRDD = javaRDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String str) throws Exception {
                return new Tuple2<>("123", str);
            }
        });

        JavaRDD<String> objRDD = pairRDD.keys();
        JavaRDD<String> strRDD = pairRDD.values();

        System.out.println(objRDD.collect().toString());
        System.out.println(strRDD.collect().toString());
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
