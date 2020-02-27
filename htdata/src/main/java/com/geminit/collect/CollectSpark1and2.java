package com.geminit.collect;

import java.io.File;
import java.io.IOException;

public class CollectSpark1and2 {
    private static File ARTIFACTS = new File(CollectUtils.ARTIFACTS_PATH);

    private static File TEMPLATES = new File(CollectUtils.HTSC_PATH, "cdap-app-templates");
    private static File ETL = new File(TEMPLATES, "cdap-etl");

    private static String PIPELINE_JAR_NAME = "cdap-data-pipeline" + "-" + CollectUtils.HTSC_VERSION + ".jar";
    private static String STREAM_JAR_NAME = "cdap-data-streams" + "-" + CollectUtils.HTSC_VERSION + ".jar";

    public static void main(String[] args) {
        System.out.println("Success: " + CollectSpark1And2());
    }

    public static boolean CollectSpark1And2() {
        // clean old spark1
        File spark1 = new File(ARTIFACTS, "spark1_2.10");
        if (!CollectUtils.deleteDir(spark1)) {
            System.out.println("删除spark1文件夹失败！");
            return false;
        }
        spark1.mkdir();

        // copy spark1
        try {
            CollectSpark1and2.CollectSpark1(spark1);
        } catch (IOException e) {
            System.out.println("收集spark1的pipeline和stream失败！");
            return false;
        }

        // clean old spark2
        File spark2 = new File(ARTIFACTS, "spark2_2.11");
        if (!CollectUtils.deleteDir(spark2)) {
            System.out.println("删除spark2文件夹失败！");
            return false;
        }
        spark2.mkdir();

        // copy spark2
        try {
            CollectSpark1and2.CollectSpark2(spark2);
        } catch (IOException e) {
            System.out.println("收集spark2的pipeline和stream失败！");
            return false;
        }

        return true;
    }

    public static void CollectSpark1(File spark1) throws IOException {
        File pipeline1Target = new File(new File(ETL, "cdap-data-pipeline"), "target");
        for (File file : pipeline1Target.listFiles()) {
            if (file.getName().equals("cdap-data-pipeline" + "-" + CollectUtils.HTSC_VERSION + ".jar")) {
                CollectUtils.copyFile(file.getPath(), new File(spark1.getPath(), PIPELINE_JAR_NAME).getPath());
            }
        }
        File stream1Target = new File(new File(ETL, "cdap-data-streams"), "target");
        for (File file : stream1Target.listFiles()) {
            if (file.getName().equals("cdap-data-streams" + "-" + CollectUtils.HTSC_VERSION + ".jar")) {
                CollectUtils.copyFile(file.getPath(), new File(spark1.getPath(), STREAM_JAR_NAME).getPath());
            }
        }
        File reportTarget = new File(new File(TEMPLATES, "cdap-program-report"), "target");
        for (File file : reportTarget.listFiles()) {
            if (file.getName().equals("cdap-program-report" + "-" + CollectUtils.HTSC_VERSION + ".jar")) {
                CollectUtils.copyFile(file.getPath(), new File(spark1.getPath(), file.getName()).getPath());
            }
        }
    }

    public static void CollectSpark2(File spark2) throws IOException {
        File pipeline2Target = new File(new File(ETL, "cdap-data-pipeline2_2.11"), "target");
        for (File file : pipeline2Target.listFiles()) {
            if (file.getName().equals("cdap-data-pipeline2_2.11" + "-" + CollectUtils.HTSC_VERSION + ".jar")) {
                CollectUtils.copyFile(file.getPath(), new File(spark2.getPath(), PIPELINE_JAR_NAME).getPath());
            }
        }
        File stream2Target = new File(new File(ETL, "cdap-data-streams2_2.11"), "target");
        for (File file : stream2Target.listFiles()) {
            if (file.getName().equals("cdap-data-streams2_2.11" + "-" + CollectUtils.HTSC_VERSION + ".jar")) {
                CollectUtils.copyFile(file.getPath(), new File(spark2.getPath(), STREAM_JAR_NAME).getPath());
            }
        }
        File reportTarget = new File(new File(TEMPLATES, "cdap-program-report"), "target");
        for (File file : reportTarget.listFiles()) {
            if (file.getName().equals("cdap-program-report" + "-" + CollectUtils.HTSC_VERSION + ".jar")) {
                CollectUtils.copyFile(file.getPath(), new File(spark2.getPath(), file.getName()).getPath());
            }
        }
    }
}
