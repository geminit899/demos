package com.geminit;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkSql {
    public static void main(String[] args) {
        SQLContext sqlContext = new SQLContext(SparkBasic.SC);

        StructType structType = getStructType();
        JavaRDD<Object> javaRDD = SparkBasic.listToRdd(Arrays.asList("a", "b", "c", "d", "e"));
        JavaRDD<Row> rowRDD = SparkSql.toRowRdd(javaRDD);

        Dataset<Row> sparkSqlDataset = sqlContext.createDataFrame(rowRDD, structType);

        sparkSqlDataset.createOrReplaceTempView("dts");
        Dataset<Row> result = sqlContext.sql("select face_id,source_type,count(face_id,source_type) from dts group by face_id,source_type");
        List<Row> list = result.collectAsList();
    }

    public static StructType getStructType() {
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("face_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("source_type", DataTypes.StringType, true));
        StructType structType = DataTypes.createStructType(structFields);

        return structType;
    }

    public static JavaRDD<Row> toRowRdd(JavaRDD javaRDD) {
        JavaRDD<Row> rowRDD = javaRDD.map(new Function<Object, Row>() {
            @Override
            public Row call(Object s) throws Exception {
                Object[] valueObjects = new Object[3];
                valueObjects[0] = "test";
                valueObjects[1] = "123";
                valueObjects[2] = "00";
                return RowFactory.create(valueObjects);
            }
        });
        return rowRDD;
    }
}
