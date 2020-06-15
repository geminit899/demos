package com.htdata.plugin.spark;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Created by tyx on 11/17/19.
 */
public class Util {

    public static void writeUTF(String str, DataOutputStream dataOut) throws IOException {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        dataOut.writeInt(bytes.length);
        dataOut.write(bytes);
    }

    public static void deleteDir(String dirPath) {
        File file = new File(dirPath);
        if (file.isFile()) {
            file.delete();
        } else {
            File[] files = file.listFiles();
            if (files == null) {
                file.delete();
            } else {
                for (int i = 0; i < files.length; i++) {
                    deleteDir(files[i].getAbsolutePath());
                }
                file.delete();
            }
        }
    }

    public static void downloadAndUnzip(String zipUrl, String unzipPath) throws IOException {
        ZipInputStream zis = null;
        //commandFile
        if (zipUrl.startsWith("file:/")) {
            // 为本地文件
            zis = new ZipInputStream(new FileInputStream(zipUrl.substring(5)));
        } else if (zipUrl.startsWith("hdfs://")) {
            // 为hdfs文件
            Pattern r = Pattern.compile("(hdfs://[^/]*)(/(.*)/([^/]*))");
            Matcher m = r.matcher(zipUrl);
            if (m.find( )) {
                String hdfsHost = m.group(1);
                String filePath = m.group(2);

                try {
                    Configuration configuration = new Configuration();
                    FileSystem hdfs = FileSystem.get(new URI(hdfsHost), configuration, "hdfs");
                    FSDataInputStream inputStream = hdfs.open(new Path(filePath));
                    zis = new ZipInputStream(inputStream);
                } catch (Exception e) {
                    throw new IOException("从hdfs下载python的zip文件出错！", e);
                }
            }
        } else {
            zis = new ZipInputStream(new FileInputStream(zipUrl));
        }

        byte[] buffer = new byte[1024];
        File folder = new File(unzipPath);
        if (!folder.exists()){
            folder.mkdir();
        }

        //get the zip file content
        ZipEntry ze = zis.getNextEntry();
        while (ze != null){
            String fileName = ze.getName();
            File newFile = new File(unzipPath + File.separator+fileName);
            //create all non exists folders
            //else you will hit FileNotFoundException for compressed folder
            //大部分网络上的源码，这里没有判断子目录
            if (ze.isDirectory()){
                newFile.mkdirs();
            }else{
                new File(newFile.getParent()).mkdirs();
                FileOutputStream fos = new FileOutputStream(newFile);
                int len;
                while ((len = zis.read(buffer))!=-1){
                    fos.write(buffer,0,len);
                }
                fos.close();
            }
            ze = zis.getNextEntry();
        }
        zis.closeEntry();
        zis.close();
    }

    public static DataType fromArrowType(ArrowType dt) {
        DataType dataType = null;
        if (dt instanceof ArrowType.Bool) {
            dataType = DataTypes.BooleanType;
        } else if (dt instanceof ArrowType.Int) {
            ArrowType.Int integer = (ArrowType.Int) dt;
            if (integer.getIsSigned() && integer.getBitWidth() == 8) {
                dataType = DataTypes.ByteType;
            } else if (integer.getIsSigned() && integer.getBitWidth() == 8 * 2) {
                dataType = DataTypes.ShortType;
            } else if (integer.getIsSigned() && integer.getBitWidth() == 8 * 4) {
                dataType = DataTypes.IntegerType;
            } else if (integer.getIsSigned() && integer.getBitWidth() == 8 * 8) {
                dataType = DataTypes.LongType;
            }
        } else if (dt instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint fl = (ArrowType.FloatingPoint) dt;
            if (fl.getPrecision() == FloatingPointPrecision.SINGLE) {
                dataType = DataTypes.FloatType;
            } else if (fl.getPrecision() == FloatingPointPrecision.DOUBLE) {
                dataType = DataTypes.DoubleType;
            }
        } else if (dt instanceof ArrowType.Utf8) {
            dataType = DataTypes.StringType;
        } else if (dt instanceof ArrowType.Binary) {
            dataType = DataTypes.BinaryType;
        } else if (dt instanceof ArrowType.Decimal) {
            ArrowType.Decimal d = (ArrowType.Decimal) dt;
            dataType = new DecimalType(d.getPrecision(), d.getScale());
        } else if (dt instanceof ArrowType.Date) {
            dataType = DataTypes.DateType;
        } else if (dt instanceof ArrowType.Timestamp) {
            dataType = DataTypes.TimestampType;
        } else {
            throw new UnsupportedOperationException("Unsupported data type: " + dt);
        }
        return dataType;
    }
}
