package com.geminit;

import org.apache.commons.lang.time.DateUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

public class SparkFunctions {
    public static void main(String[] args) {

    }

    public static void joinTestFunction() {
        JavaRDD<String> javaRDD1 = SparkBasic.listToRdd(Arrays.asList("a", "b", "c", "d", "e"));
        JavaRDD<Integer> javaRDD2 = SparkBasic.listToRdd(Arrays.asList(1, 2, 3, 4, 5));
        JavaPairRDD<String, String> pairRDD1 = mapToPairFunction(javaRDD1);
        JavaPairRDD<String, Integer> pairRDD2 = mapToPairFunction(javaRDD2);

        JavaPairRDD<String, Tuple2<String, Integer>> test2 = pairRDD1.join(pairRDD2);
    }

    public static JavaRDD mapFunction(JavaRDD javaRDD) {
        JavaRDD rowRDD = javaRDD.map(new Function<Object, Object>() {
            @Override
            public Object call(Object object) throws Exception {
                return object;
            }
        });
        return rowRDD;
    }

    public static JavaPairRDD mapValuesFunction(JavaPairRDD javaPairRDD) {
        JavaPairRDD<Object, Object> test = javaPairRDD.mapValues(new Function<Object, Object>() {
            @Override
            public Object call(Object s) throws Exception {
                return null;
            }
        });
        return test;
    }

    public static JavaPairRDD mapToPairFunction(JavaRDD javaRDD) {
        JavaPairRDD javaPairRDD = javaRDD.mapToPair(new PairFunction() {
            @Override
            public Tuple2 call(Object object) throws Exception {
                return new Tuple2("test", object);
            }
        });
        return javaPairRDD;
    }

    public static JavaPairRDD reduceByKeyFunction(JavaPairRDD javaPairRDD) {
        JavaPairRDD<String, String> reduceByKey = javaPairRDD.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s + "-" + s2;
            }
        });
        return reduceByKey;
    }

    public static void streamForeachFunction(JavaDStream javaDStream) {
        javaDStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> javaRDD) throws Exception {
                javaRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
                    @Override
                    public void call(Iterator<String> iterator) throws Exception {
                        String sss;
                        while (iterator.hasNext()) {
                            //2019-01-02 17:45:00__2019-01-02 17:50:00__001__imsi,face__1,2__5__1000
                            String[] kafka = iterator.next().split("__");
                            sss = kafka[0];
                        }
                    }
                });
            }
        });
    }
}
