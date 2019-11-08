package com.geminit.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.RunJar;

import java.io.File;

/**
 * Created by root on 7/26/19.
 */
public class YarnUnjarTest {
    public static void main(String[] args) throws Exception {
        File inFile = new File("/home/test.jar");
        File outFile = new File("/home/test");
        RunJar.unJar(inFile, outFile);
    }
}
