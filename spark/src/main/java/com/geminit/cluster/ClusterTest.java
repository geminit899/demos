package com.geminit.cluster;

import org.apache.spark.deploy.SparkSubmit;

import java.util.ArrayList;
import java.util.List;

public class ClusterTest {
    public static void main(String[] args) {
        List<String> builder = new ArrayList<>();
        builder.add("--master");
        builder.add("yarn");
        builder.add("--deploy-mode");
        builder.add("cluster");

        if (args.length > 0) {
            System.out.println("args.length > 0");
            builder.add("--conf");builder.add("spark.hadoop.yarn.resourcemanager.hostname=htsp.htdata.com");
            builder.add("--conf");builder.add("spark.hadoop.yarn.resourcemanager.address=htsp.htdata.com:8050");
            builder.add("--conf");builder.add("spark.hadoop.yarn.resourcemanager.scheduler.address=htsp.htdata.com:8030");
            builder.add("--conf");builder.add("spark.hadoop.yarn.resourcemanager.webapp.address=htsp.htdata.com:8088");
            builder.add("--conf");builder.add("spark.hadoop.yarn.resourcemanager.webapp.https.address=htsp.htdata.com:8090");
            builder.add("--conf");builder.add("spark.hadoop.yarn.resourcemanager.resource-tracker.address=htsp.htdata.com:8025");
            builder.add("--conf");builder.add("spark.hadoop.yarn.resourcemanager.admin.address=htsp.htdata.com:8141");
        } else {
            System.out.println("args.length = 0");
        }

        builder.add("--class");
        builder.add("com.geminit.SparkBasic");
        builder.add("/home/htsc/spark-1.0.jar");

        String[] main = new String[builder.size()];
        for (int i = 0; i < builder.size(); i++) {
            main[i] = builder.get(i);
        }

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
