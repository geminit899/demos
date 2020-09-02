package com.geminit.decisiontree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.apache.spark.SparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag$;

public class DecisionTreeTest {
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

        JavaRDD<LabeledPoint> data = javaRDD.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String record) throws Exception {
                List<Double> featureList = new ArrayList<>();
                List<Integer> featureIndex = new ArrayList<>();
                int counter = 0;
                for (String field : featuresList.keySet()) {
                    if (record.get(field) != null) {
                        featureList.add(((Number) record.get(field)).doubleValue());
                        featureIndex.add(counter);
                    }
                    counter++;
                }

                return new LabeledPoint((Double) record.get(config.labelField),
                        Vectors.sparse(counter, Ints.toArray(featureIndex), Doubles.toArray(featureList)));
            }
        });

        // Split the data into training and test sets (30% held out for testing)
        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
        JavaRDD<LabeledPoint> trainingData = splits[0];
        JavaRDD<LabeledPoint> testData = splits[1];

        // Set parameters.
        //  Empty categoricalFeaturesInfo indicates all features are continuous.
        Integer numClasses = 2;
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        String impurity = "gini";
        Integer maxDepth = 5;
        Integer maxBins = 32;

        // Train a DecisionTree model for classification.
        final DecisionTreeModel model = DecisionTree.trainClassifier(trainingData, numClasses,
                categoricalFeaturesInfo, impurity, maxDepth, maxBins);

        // Evaluate model on test instances and compute test error
        JavaPairRDD<Double, Double> predictionAndLabel =
                testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                    @Override
                    public Tuple2<Double, Double> call(LabeledPoint p) {
                        return new Tuple2<>(model.predict(p.features()), p.label());
                    }
                });
        Double testErr =
                1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Double, Double> pl) {
                        return !pl._1().equals(pl._2());
                    }
                }).count() / testData.count();

        System.out.println("Test Error: " + testErr);
        System.out.println("Learned classification tree model:\n" + model.toDebugString());

        // Save and load model
//        model.save(con.sc(), "target/tmp/myDecisionTreeClassificationModel");
//        DecisionTreeModel sameModel = DecisionTreeModel
//                .load(jsc.sc(), "target/tmp/myDecisionTreeClassificationModel");
    }
}
