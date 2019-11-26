package com.geminit;

public class Emitter {
    private String outPath = "???";

    public void emit(String outPath) {
        this.outPath = outPath;
    }

    public String get() {
        return outPath;
    }
}
