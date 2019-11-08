package com.geminit.batch;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class EEBasic {
    public static ExecutionEnvironment EE = ExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) throws Exception {
        DataSet<Object> dataSet = getDataSetFromElements("123");

        DataSetFunctions.mapFuntion(dataSet);

        EE.execute();
    }

    public static ExecutionConfig getConfig() {
        return EE.getConfig();
    }

    public static DataSet<Object> getDataSetFromElements(Object ...objects) {
        return EE.fromElements(objects);
    }

    public static DataSet<Tuple2<Object, Object>> getDataSetPairFromElements(Tuple2<Object, Object> ...tuple2s) {
        return EE.fromElements(tuple2s);
    }

}
