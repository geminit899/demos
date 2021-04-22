package com.geminit;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.mllib.fpm.PrefixSpan;
import org.apache.spark.mllib.fpm.PrefixSpanModel;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class demo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("zpsb");
        conf.setMaster("local");
        SparkContext sc = new SparkContext(conf);

        List<List<List<String>>> list = Arrays.asList(
            Arrays.asList(Arrays.asList("at", "b"), Arrays.asList("g", "d", "c")),
            Arrays.asList(Arrays.asList("a"), Arrays.asList("c", "d", "at"), Arrays.asList("a", "b")),
            Arrays.asList(Arrays.asList("a", "at"), Arrays.asList("a", "b", "e")),
            Arrays.asList(Arrays.asList("f"), Arrays.asList("f", "g", "h"))
        );
        Seq<List<List<String>>> tmpSeq = JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
        //并行集合，是通过对于驱动程序中的集合调用JavaSparkContext.parallelize来构建的RDD
        JavaRDD<List<List<String>>> sequences = sc.parallelize(tmpSeq, 2, ClassTag$.MODULE$.apply(list.getClass())).toJavaRDD();

        PrefixSpan prefixSpan = new PrefixSpan()
                .setMinSupport(0.5)
                .setMaxPatternLength(3).setMaxLocalProjDBSize(10);
        PrefixSpanModel<String> model = prefixSpan.run(sequences);
        for (PrefixSpan.FreqSequence<String> freqSeq: model.freqSequences().toJavaRDD().collect()) {
            System.out.println(freqSeq.javaSequence() + ", " + freqSeq.freq());
        }
    }
}
