package com.geminit.collect;

import java.io.File;
import java.io.IOException;

/**
 * Created by tyx on 02/25/20.
 */
public class CollectPluginJars {
    private static int pluginFileNum = 0;

    public static void main(String[] args) {
        long begin = System.currentTimeMillis();
        System.out.println("开始拷贝！");

        // clean old artifacts
        File artifacts = new File(CollectUtils.ARTIFACTS_PATH);
        if (!CollectUtils.deleteDir(artifacts)) {
            System.out.println("删除artifacts文件夹失败！");
            return;
        }
        artifacts.mkdir();

        // copy Spark 1 and 2
        CollectSpark1and2.CollectSpark1And2();

        // copy hydrators
        pluginFileNum = 0;
        File rootFile = new File(CollectUtils.HYDRATOR_PATH);
        //获取Hydrator根目录下所有文件
        for (File plugin : rootFile.listFiles()) {
            //对所有插件目录进行操作
            if (plugin.isDirectory()) {
                collectPluginJarsAndJsons(plugin, artifacts.getPath());
            }
        }

        long end = System.currentTimeMillis();
        long period = end - begin;
        System.out.println("拷贝结束！共拷贝 " + pluginFileNum + " 个插件文件，耗费 " + period + " 毫秒。");
        pluginFileNum = 0;
    }

    public static void collectPluginJarsAndJsons(File file, String destinationPath) {
        if (CollectUtils.isDept(file)) {
            for (File files : file.listFiles()) {
                if (files.isDirectory() && files.getName().equals("target")) {
                    //遍历target目录下所有文件
                    for (File targetFiles : files.listFiles()) {
                        //获取pluginName相符的jar文件和json文件
                        if (targetFiles.getName().contains(file.getName()) && !targetFiles.getName().contains("sources.jar") &&
                                !targetFiles.getName().contains("javadoc.jar") &&
                                (targetFiles.getName().endsWith(".jar") || targetFiles.getName().endsWith(".json"))) {
                            try {
                                String filePath = targetFiles.getAbsolutePath();
                                String fileName = targetFiles.getName();
                                CollectUtils.copyFile(filePath, destinationPath + targetFiles.separator + fileName);
                                pluginFileNum++;
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
                    collectPluginJarsAndJsons(files, destinationPath);
                }
            }
        }
    }

}
