package com.geminit;

import java.util.Date;

public class Test {
    public static void main(String[] args) throws Exception {
        String path = "/Users/geminit/IdeaProjects/Demos/py4j/src/main/python/test.py";
        String modal = "/Users/geminit/PycharmProjects/Tensorflow-demo/log/example";

        int port = 6004;

        Process process;
        do {
            port += 1;
            process = Runtime.getRuntime().exec(new String[]{"python3", path, modal, String.valueOf(port)});
            Thread.sleep(5000);
        } while (!process.isAlive());

        System.out.println(port);
    }


}
