package com.htdata.plugin;

import com.htdata.plugin.spark.PythonCompute;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by tyx on 11/15/19.
 */
public class SparkTest {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setAppName("zpsb");
        conf.setMaster("local");
        SparkContext sc = new SparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        List<Row> rows = new ArrayList<>();
        String[] names = Arrays.asList("tyx", "hy", "zp", "lf", "xq", "lls", "tls").toArray(new String[5]);
        for (int i = 0; i < 7; i++) {
            Object[] valueObjects = new Object[2];
            valueObjects[0] = names[i];
            valueObjects[1] = 20 + i;
            rows.add(RowFactory.create(valueObjects));
        }
        Seq<Row> tmpSeq = JavaConverters.asScalaIteratorConverter(rows.iterator()).asScala().toSeq();
        //并行集合，是通过对于驱动程序中的集合调用JavaSparkContext.parallelize来构建的RDD
        JavaRDD<Row> javaRDD = sc.parallelize(tmpSeq, 2, ClassTag$.MODULE$.apply(Row.class)).toJavaRDD();

        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType inputStructType = DataTypes.createStructType(structFields);

        //根据rowJavaRDD和structType构建personsDF的Dataset
        Dataset<Row> inputDataset = sqlContext.createDataFrame(javaRDD, inputStructType);

        boolean needAction = true;
        if (needAction) {
            inputDataset = inputDataset.repartition(1);
        }

        PythonCompute pythonCompute = new PythonCompute();
        String path = "file:/Users/geminit/PycharmProjects/PythonDemo/spark/pyspark.zip";
        JavaPairRDD<StructType, Row> resultPair = pythonCompute.compute(path, inputDataset);

        JavaRDD<String> result = resultPair.map(new Function<Tuple2<StructType, Row>, String>() {
            @Override
            public String call(Tuple2<StructType, Row> tuple2) throws Exception {
                for (int i = 0; i < tuple2._1().size(); i++) {
                    tuple2._2().get(tuple2._1().fieldIndex("name"));
                }
                return "name is ";
            }
        });

        System.out.println(result.count());
    }
}
