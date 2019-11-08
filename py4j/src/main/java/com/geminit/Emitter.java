package com.geminit;

import com.google.gson.Gson;
import org.apache.spark.rdd.RDD;

import java.util.HashMap;
import java.util.Map;

public class Emitter {
    private String outPath = "???";

    public void emit(String outPath) {
        this.outPath = outPath;
    }

    public String get() {
        return outPath;
    }
}
