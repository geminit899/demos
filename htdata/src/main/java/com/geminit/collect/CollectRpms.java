package com.geminit.collect;

import java.io.File;
import java.io.IOException;

/**
 * Created by tyx on 02/25/20.
 */
public class CollectRpms {
    private static int rpmFileNum = 0;

    public static void main(String[] args) {
        long begin = System.currentTimeMillis();
        System.out.println("开始拷贝！");

        // clean old rpms
        File rpms = new File(CollectUtils.RPMS_PATH);
        if (!CollectUtils.deleteDir(rpms)) {
            System.out.println("删除rpms文件夹失败！");
            return;
        }
        rpms.mkdir();

        // copy rpms
        rpmFileNum = 0;
        File rootFile = new File(CollectUtils.HTSC_PATH);
        //获取HTSC根目录下所有文件
        for (File module : rootFile.listFiles()) {
            //对所有目录进行操作
            if (module.isDirectory()) {
                copyRpms(module, rpms.getPath());
            }
        }

        long end = System.currentTimeMillis();
        long period = end - begin;
        System.out.println("拷贝结束！共拷贝 " + rpmFileNum + " 个rpm文件，耗费 " + period + " 毫秒。");
        rpmFileNum = 0;
    }

    public static void copyRpms(File file, String destinationPath) {
        if (CollectUtils.isDept(file)) {
            for (File files : file.listFiles()) {
                if (files.isDirectory() && files.getName().equals("target")) {
                    //遍历target目录下所有文件
                    for (File targetFiles : files.listFiles()) {
                        //获取pluginName相符的jar文件和json文件
                        if (targetFiles.isFile() && targetFiles.getName().endsWith(".rpm")) {
                            try {
                                String filePath = targetFiles.getAbsolutePath();
                                String fileName = targetFiles.getName();
                                CollectUtils.copyFile(filePath, destinationPath + targetFiles.separator + fileName);
                                rpmFileNum++;
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        } else {
            for (File files : file.listFiles()) {
                if (files.isDirectory()) {
                    copyRpms(files, destinationPath);
                }
            }
        }
    }
}
