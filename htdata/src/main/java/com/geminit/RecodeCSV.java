package com.geminit;

import java.io.*;

public class RecodeCSV {
    private static String csvPath = "/Users/geminit/Desktop/CONS_PQ_DAY_201901_no-name.csv";

    public static void main(String[] args) {
        try {
            File csv = new File(csvPath);
            //先FileReader把文件读出来再bufferReader按行读  reader.readLine(); 没有标题用不着了
            BufferedReader reader = new BufferedReader(new FileReader(csv));
            FileInputStream inputStream = new FileInputStream(csv);
            byte[] bytes  = new byte[1024];
            int len = inputStream.read(bytes);
            while(len != -1) {
                String str = new String(bytes, "GB2312");
                len = inputStream.read(bytes);//当未读取到文件末尾时 继续读取下3个字节
            }
            reader.close();
        } catch (Exception e) {
            //在命令行打印异常信息在程序中出错的位置及原因。
            e.printStackTrace();
        }
    }
}
