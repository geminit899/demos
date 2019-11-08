package com.geminit;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Batch Flink OutputFormat.
 * @param <T> type of object in the collection
 */
public class FlinkOutputFormat<T> implements OutputFormat<T> {
    @Override
    public void configure(Configuration configuration) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
    }

    @Override
    public void writeRecord(T t) throws IOException {
        if (t instanceof Tuple2) {
            Tuple2<Object, Object> tuple2 = (Tuple2<Object, Object>) t;

            String str = tuple2.f0.toString() + " - " + tuple2.f1.toString();

            FileOutputStream fos = new FileOutputStream("/Users/geminit/Desktop/test.txt");
            fos.write(str.getBytes());
            fos.close();
        }
    }

    @Override
    public void close() throws IOException {
    }
}
