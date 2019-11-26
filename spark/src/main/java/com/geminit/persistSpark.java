package com.geminit;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tyx on 11/14/19.
 */
public class persistSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("zpsb");
        conf.setMaster("local");
        SparkContext sc = new SparkContext(conf);

        List<Map<String, Object>> list = new ArrayList<>();
        String[] names = Arrays.asList("tyx", "hy", "zp", "lf", "xq").toArray(new String[5]);
        for (int i = 0; i < 5; i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("name", names[i]);
            map.put("age", 20 + i);
            list.add(map);
        }
        Seq<Map<String, Object>> tmpSeq = JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
        //并行集合，是通过对于驱动程序中的集合调用JavaSparkContext.parallelize来构建的RDD
        RDD<Map<String, Object>> rdd = sc.parallelize(tmpSeq, 1, ClassTag$.MODULE$.apply(Map.class));
        rdd.saveAsTextFile("/home/rdds/1");
    }
}
