package com.geminit.kmeans;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KmeasTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("zpsb");
        conf.setMaster("local");
        SparkContext context = new SparkContext(conf);

        List<String> list = new ArrayList<>();
        list.add("car");
        list.add("imsi");
        list.add("face");
        Seq<String> tmpSeq = JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
        //并行集合，是通过对于驱动程序中的集合调用JavaSparkContext.parallelize来构建的RDD
        JavaRDD<String> javaRDD = context.parallelize(tmpSeq, 2, ClassTag$.MODULE$.apply(String.class)).toJavaRDD();

        Map<String, Integer> featuresList = new HashMap<>();
        featuresList.put("a", 1);
        featuresList.put("b", 2);
        featuresList.put("c", 3);

        JavaRDD<Vector> data = javaRDD.map(new Function<String, Vector>() {
            @Override
            public Vector call(String record) throws Exception {
                double[] values = new double[3];
                values[0] = 1.0;
                values[1] = 2.0;
                values[2] = 3.0;
                return Vectors.dense(values);
            }
        });

        KMeansModel model = KMeans.train(data.rdd(), 1, 10);
        String pmml = model.toPMML();
        System.out.println(pmml);
    }
}
