package com.geminit;

import org.apache.spark.rdd.RDD;

import java.util.List;
import java.util.Map;

public interface MainInterface {
    public void initSparkContext();

    public void toPython(String rddPath, Emitter emitter);
}
