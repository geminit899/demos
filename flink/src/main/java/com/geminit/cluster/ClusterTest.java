package com.geminit.cluster;

import org.apache.flink.client.cli.CliFrontend;

import java.util.concurrent.CountDownLatch;

public class ClusterTest {
    public static void main(String[] args) {
        String[] main = {"run", "-c", "com.geminit.batch.BatchTest", "/Users/geminit/IdeaProjects/Demos/flink/target/flink-1.0.jar"};

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
        Thread task = new Thread() {
            @Override
            public void run() {
                CliFrontend.main(main);
                System.out.println(456);
            }
        };

        try {
            task.start();
            task.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            System.out.println(789);
        }
    }
}

