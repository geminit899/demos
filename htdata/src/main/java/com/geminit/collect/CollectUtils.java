package com.geminit.collect;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class CollectUtils {
    public static String HTSC_VERSION = "4.0";
    public static String CDAP_VERSION = "6.2.0";
    public static String HYDRATOR_VERSION = "2.4.0";

    public static String HTSC_PATH = "/Volumes/Work/SVN/HTSC-" + HTSC_VERSION;
    public static String HYDRATOR_PATH = HTSC_PATH + "/hydrator" + HYDRATOR_VERSION;
    public static String ARTIFACTS_PATH = HTSC_PATH + "/artifacts";
    public static String RPMS_PATH = HTSC_PATH + "/htsc";


    public static boolean isDept(File file) {
        for (File files : file.listFiles()) {
            if (files.isDirectory()) {
                for (File sonFiles : files.listFiles()) {
                    if (sonFiles.isFile() && sonFiles.getName().equals("pom.xml")) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public static void copyFile(String oldPath, String newPath) throws IOException {
        File oldFile = new File(oldPath);
        File file = new File(newPath);
        FileInputStream in = new FileInputStream(oldFile);
        FileOutputStream out = new FileOutputStream(file);

        byte[] buffer=new byte[2097152];
        int readByte = 0;
        while((readByte = in.read(buffer)) != -1){
            out.write(buffer, 0, readByte);
        }

        in.close();
        out.close();
    }

    public static boolean deleteDir(File dir) {
        if (!dir.exists()) return true;
        if (dir.isDirectory()) {
            String[] childrens = dir.list();
            // 递归删除目录中的子目录下
            for (String child : childrens) {
                boolean success = deleteDir(new File(dir, child));
                if (!success) return false;
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }
}
