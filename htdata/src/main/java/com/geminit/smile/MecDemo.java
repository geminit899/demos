package com.geminit.smile;

import smile.clustering.DeterministicAnnealing;
import smile.clustering.MEC;
import smile.data.CategoricalEncoder;
import smile.data.DataFrame;
import smile.data.Tuple;
import smile.data.formula.Formula;
import smile.data.type.DataTypes;
import smile.data.type.StructField;
import smile.data.type.StructType;
import smile.math.distance.EuclideanDistance;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MecDemo {
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
        double[][] doubles = new double[2000][2];
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 500; j++) {
                double d = random.nextDouble();
                if (d >= 0.5) {
                    d = d - 1;
                }
                doubles[i * 5 + j][0] = i * 100 + d;
                doubles[i * 5 + j][1] = i * 100 + d;
            }
        }
        List<Tuple> rows = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 500; j++) {
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

        MEC<double[]> mec = MEC.fit(data, new EuclideanDistance(), 4, 10);
        double[] test = new double[2];
        test[0] = 1.5;
        test[1] = 1.5;
        int predict = mec.predict(test);

//        double[][] attractors = denclue.attractors;
//        for (double[] attractor : attractors) {
//            System.out.println("centroid: " + attractor[0] + ", " + attractor[1]);
//            for (double[] d : doubles) {
//                if (d[0] == attractor[0] && d[1] == attractor[1]) {
//                    System.out.println("\td: " + d[0] + ", " + d[1]);
//                }
//            }
//        }

        System.out.println(0);
    }
}
