package com.geminit.smile;

import org.apache.commons.csv.CSVFormat;
import smile.clustering.GMeans;
import smile.clustering.KMeans;
import smile.clustering.XMeans;
import smile.data.CategoricalEncoder;
import smile.data.DataFrame;
import smile.data.Tuple;
import smile.data.formula.Formula;
import smile.data.type.DataTypes;
import smile.data.type.StructField;
import smile.data.type.StructType;
import smile.io.Read;
import smile.manifold.LLE;
import smile.manifold.TSNE;
import smile.projection.PCA;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class LLEDemo {
    public static void main(String[] args) throws Exception {
        CSVFormat format = CSVFormat.DEFAULT.withDelimiter('\t');
        DataFrame dataFrame = Read.csv("/home/geminit/下载/smile-master/shell/src/universal/data/manifold/swissroll.txt", format);
        double[][] data = dataFrame.toArray(false, CategoricalEncoder.ONE_HOT);

        TSNE tsne = new TSNE(data, 2, 20, 200, 100);
        TSNE tsne1 = new TSNE(data, 3, 20, 200, 100);
        double[][] vertices = tsne.coordinates;

        System.out.println(0);
    }
}
