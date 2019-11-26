package com.geminit.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * Created by tyx on 11/20/19.
 */
public class HDFSADMS {


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        String hdfsHost = "hdfs://192.168.0.114:8020";
        FileSystem hdfs = FileSystem.get(new URI(hdfsHost), configuration, "cdap");

        HDFSUtils utils = new HDFSUtils();
        utils.uploadFile(hdfs, "/home/python.zip", "/cdap/python/python.zip");
//        utils.downloadFile(hdfs, "", "");
//        utils.deleteFile(hdfs, "");
    }
}

class HDFSUtils {
    public void downloadFile(FileSystem hdfs, String sourcePath, String destPath) throws Exception {
        FSDataInputStream inputStream = hdfs.open(new Path(sourcePath));
        FileOutputStream outputStream = new FileOutputStream(new File(destPath));
        IOUtils.copyBytes(inputStream, outputStream, hdfs.getConf(), true);

        System.out.println("下载完成");
        //关闭资源
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(outputStream);
    }

    public void uploadFile(FileSystem hdfs, String sourcePath, String destPath) throws Exception {
        FileInputStream inputStream = new FileInputStream(new File(sourcePath));
        FSDataOutputStream outputStream = hdfs.create(new Path(destPath), true);
        IOUtils.copyBytes(inputStream, outputStream, hdfs.getConf(), true);

        System.out.println("上传完成");
        //关闭资源
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(outputStream);
    }

    public void deleteFile(FileSystem hdfs, String filePath) throws Exception {
        boolean delete = hdfs.delete(new Path(filePath), true);
        if (delete) {
            System.out.println("成功删除");
        } else {
            System.out.println("删除失败");
        }
    }
}