package com.geminit.cluster;

import org.apache.flink.client.cli.CliFrontend;
import org.apache.spark.deploy.SparkSubmit;

public class ClusterTest {
    public static void main(String[] args) {
        String[] main = {"--class", "com.geminit.SparkBasic", "/Users/geminit/IdeaProjects/Demos/flink/target/flink-1.0.jar"};

        Runnable task = new Runnable() {
            @Override
            public void run() {
                submit(main);
                System.out.println(456);
            }
        };

        try {
            new Thread(task).start();
        } finally {
            System.out.println(123);
        }

    }

    public static void submit(String[] main) {
        SparkSubmit.main(main);
    }
}
