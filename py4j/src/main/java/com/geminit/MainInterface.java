package com.geminit;

import java.util.List;
import java.util.Map;

public interface MainInterface {
    public void transform(Map dic, Emitter emitter);

    public void comput(List<Map> maps, Emitter emitter);
}
