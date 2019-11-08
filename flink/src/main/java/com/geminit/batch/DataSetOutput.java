package com.geminit.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class DataSetOutput {
    public static void main(String[] args) {
		DataSet<Tuple2<Object, Object>> dataSet = EEBasic.getDataSetPairFromElements(new Tuple2<Object, Object>("123", "123"));
    }

    public static DataSink saveDataSetAsText(DataSet dataSet) {
        return dataSet.writeAsText("/Users/geminit/Desktop/test.txt");
    }
}
