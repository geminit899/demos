package com.geminit.mllib.gbm;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.impurity.Gini;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;

import java.util.HashMap;

public class GbmTraining {
    private static String defaultSource = "/home/geminit/work/opensource/spark-2.1.1/data/mllib/sample_libsvm_data.txt";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("zpsb");
        conf.setMaster("local");
        SparkContext context = new SparkContext(conf);

        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(context, defaultSource).toJavaRDD();


        JavaRDD<LabeledPoint>[] split = data.randomSplit(new double[] {0.7, 0.3});
        JavaRDD<LabeledPoint> train = split[0];
        JavaRDD<LabeledPoint> test = split[1];


        BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Regression");
        boostingStrategy.setLearningRate(boostingStrategy.getLearningRate());
        boostingStrategy.setNumIterations(10);
        boostingStrategy.treeStrategy().setMaxDepth(5);
        boostingStrategy.treeStrategy().setMaxBins(10);
        boostingStrategy.treeStrategy().setNumClasses(2);
        boostingStrategy.treeStrategy().setMinInfoGain(1e-4);
        boostingStrategy.treeStrategy().setImpurity(Gini.instance());
        boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(new HashMap<>());

        GradientBoostedTreesModel model = GradientBoostedTrees.train(train, boostingStrategy);

        JavaPairRDD<Double, Double> predictionAndLabel =
                test.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                    @Override
                    public Tuple2<Double, Double> call(LabeledPoint p) throws Exception {
                        Vector feature = p.features();
                        double t1 = model.predict(feature);
                        double t2 = p.label();
                        return new Tuple2<>(t1, t2);
                    }
                });

        double testMSE = predictionAndLabel.mapToDouble(pl -> {
            double diff = pl._1() - pl._2();
            return diff * diff;
        }).mean();

        System.out.println("Test Mean Squared Error: " + testMSE);
        System.out.println("Learned regression GBT model: \n" + model.toDebugString());


        String savePath = "/home/geminit/work/myModel";
        model.save(context, savePath);
    }
}
