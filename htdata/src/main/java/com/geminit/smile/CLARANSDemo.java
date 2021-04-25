package com.geminit.smile;

import smile.clustering.CLARANS;
import smile.data.CategoricalEncoder;
import smile.data.DataFrame;
import smile.data.Tuple;
import smile.data.formula.Formula;
import smile.data.type.DataTypes;
import smile.data.type.StructField;
import smile.data.type.StructType;
import smile.math.MathEx;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CLARANSDemo {
    public static void main(String[] args) {
        List<String> labelList = new ArrayList<>();
        labelList.add("a");
        labelList.add("b");
        labelList.add("c");
        labelList.add("d");

        StructType schema = DataTypes.struct(
            new StructField("class", DataTypes.IntegerType),
            new StructField("V1", DataTypes.DoubleType),
            new StructField("V2", DataTypes.DoubleType)
        );
        Random random = new Random(System.currentTimeMillis());
        double[][] doubles = new double[20][2];
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 5; j++) {
                double d = random.nextDouble();
                if (d >= 0.5) {
                    d = d - 1;
                }
                doubles[i * 5 + j][0] = i + d;
                doubles[i * 5 + j][1] = i + d;
            }
        }
        List<Tuple> rows = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 5; j++) {
                Object[] row = new Object[3];
                row[0] = i;
                row[1] = doubles[i * 5 + j][0];
                row[2] = doubles[i * 5 + j][1];
                rows.add(Tuple.of(row, schema));
            }
        }

        Formula formula = Formula.lhs("class");
        DataFrame dataFrame = DataFrame.of(rows, schema);
        double[][] data = formula.x(dataFrame).toArray(false, CategoricalEncoder.ONE_HOT);

        CLARANS<double[]> clarans = CLARANS.fit(data, MathEx::distance, 4, 20);

        double[][] centroids = clarans.centroids;

        for (double[] centroid : centroids) {
            System.out.println("centroid: " + centroid[0] + ", " + centroid[1]);
            for (double[] d : doubles) {
                if (d[0] == centroid[0] && d[1] == centroid[1]) {
                    System.out.println("\td: " + d[0] + ", " + d[1]);
                }
            }
        }

        System.out.println(0);
    }
}
