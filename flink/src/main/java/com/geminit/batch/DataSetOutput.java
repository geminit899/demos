package com.geminit.batch;

import com.geminit.db.DBRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;

public class DataSetOutput {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment ee = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> source = ee.fromElements("123");
        DataSet<Tuple2<String, String>> dataSet = source.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                return new Tuple2<>("message", s);
            }
        });
		DataSet<Tuple2<DBRecord, String>> dealData = dataSet.map(
		        new MapFunction<Tuple2<String, String>, Tuple2<DBRecord, String>>() {
            @Override
            public Tuple2<DBRecord, String> map(Tuple2<String, String> tuple2) throws Exception {
                return new Tuple2<>(new DBRecord(tuple2.f1), "WTF");
            }
        });
		saveDataSetByHadoopOutputFormat(dealData);
		ee.execute();
    }

    public static DataSink saveDataSetAsText(DataSet<Tuple2<String, String>> dataSet) {
        return dataSet.writeAsText("/Users/geminit/Desktop/test.txt");
    }

    public static DataSink saveDataSetByHadoopOutputFormat(DataSet<Tuple2<DBRecord, String>> dataSet) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("mapred.output.dir", "/home/geminit/work/flink");

        Class clz = Thread.currentThread().getContextClassLoader().loadClass("com.geminit.db.ETLDBOutputFormat");
        OutputFormat jobFormat = (OutputFormat) clz.newInstance();

        return dataSet.output(new HadoopOutputFormat<DBRecord, String>(jobFormat, new Job(configuration)));
    }
}
