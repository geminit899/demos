package com.geminit.mllib.gbm;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;

public class GbmPrediction {
    private static String myModel = "/home/geminit/work/myModel";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("zpsb");
        conf.setMaster("local");
        SparkContext context = new SparkContext(conf);

        GradientBoostedTreesModel model = GradientBoostedTreesModel.load(context, myModel);

        double[] feature = new double[2];
        Vector featureScore = Vectors.dense(feature);

    }
}
