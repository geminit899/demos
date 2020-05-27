package com.geminit;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

public class CombineCSV {
    private static String csvPath = "/Users/geminit/Desktop/电力项目/NanShanShiTotal";
    private static String desCsv = "/Users/geminit/Desktop/电力项目/LongShanShi.csv";

    public static void main(String[] args) {
        File csvPath = new File(CombineCSV.csvPath);
        System.out.println("开始查找csv文件。");
        List<File> csvs = findCsvs(csvPath);
        System.out.println("共找到" + csvs.size() + "个csv文件。\n开始组合……");
        combineCsvs(csvs);
        System.out.println("组合完毕。");
    }

    public static List<File> findCsvs(File path) {
        List<File> csvs = new ArrayList<>();
        for (File file : path.listFiles()) {
            if (file.isFile()) {
                if (file.getName().startsWith("part-r-")) {
                    csvs.add(file);
                }
            } else {
                csvs.addAll(findCsvs(file));
            }
        }
        return csvs;
    }

    public static void combineCsvs(List<File> csvs) {
        try {
            File des = new File(CombineCSV.desCsv);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(des, true),"gb2312"));

            writer.write("\"县区\",\"乡镇\",\"村名称\",\"用户标识\",\"台区标识\",\"台区编号\",\"台区名称\"," +
                    "\"1月用电量\",\"2月用电量\",\"3月用电量\",\"4月用电量\",\"5月用电量\",\"6月用电量\"," +
                    "\"7月用电量\",\"8月用电量\",\"9月用电量\",\"10月用电量\",\"11月用电量\",\"12月用电量\"\n");

            for (File csv : csvs) {
                try {
                    //先FileReader把文件读出来再bufferReader按行读  reader.readLine(); 没有标题用不着了
                    BufferedReader reader = new BufferedReader(new FileReader(csv));
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        String[] cols = line.split(",", -1);
                        line = "";
                        for (String col : cols) {
                            line += "\"" + col + "\",";
                        }
                        line = line.substring(0, line.length() - 1);
                        writer.write(line + "\n");
                    }
                    reader.close();
                } catch (Exception e) {
                    //在命令行打印异常信息在程序中出错的位置及原因。
                    e.printStackTrace();
                }
                writer.flush();
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
