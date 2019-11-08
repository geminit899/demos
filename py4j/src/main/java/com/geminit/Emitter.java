package com.geminit;

import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;

public class Emitter {
    private Map<String, Object> map;

    public Emitter() {
        map = new HashMap();
    }

    public void emit(String str) {
        Gson gson = new Gson();
        map = gson.fromJson(str, map.getClass());
    }

    public Map<String, Object> get() {
        return map;
    }
}
