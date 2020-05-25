package com.geminit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

/**
 * @Author: XuQi
 * @Date: Created in 2018/2/28 10:44
 */
public class HbaseUtil {
    public static Configuration configuration;//管理Hbase的配置信息

    public static Connection connection;//管理Hbase连接

    public static Admin admin;//管理Hbase数据库的表信息


    public static void init() throws FileNotFoundException {
        configuration = HBaseConfiguration.create();
//        configuration.addResource(new FileInputStream("/home/ideaProjects/Demos/hbase/src/main/resources/hbase-site.xml"));
//        configuration.set("hbase.zookeeper.quorum", "192.168.0.114");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            System.err.println("connected!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }
            System.err.println("closed!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        init();
        createTable("student", new String[]{"score"});
        insertData("student", "zhangsan", "score", "English", "60");
        insertData("student", "zhangsan", "score", "Math", "70");
        insertData("student", "zhangsan", "score", "Computer", "80");
        insertData("student", "lisi", "score", "English", "70");
        insertData("student", "lisi", "score", "Math", "70");
        insertData("student", "lisi", "score", "Computer", "90");

//        Table table = connection.getTable(TableName.valueOf("cdap_system:configuration"));
//        Get get = new Get(Bytes.toBytes("DEFAULT"));
//        get.addColumn(Bytes.toBytes("f"), Bytes.toBytes("system.program.state.retry.policy.max.retries"));
//        Result result = table.get(get);

//        getData("cdap_system:configuration", "DEFAULT", "f", "system.program.state.retry.policy.max.retries");

        close();
    }

    /**
     *
     *@Param myTableName 表名
     *@Param cols 列族数组
     *
     **/
    public static void createTable(String myTableName, String[] cols) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            System.err.println("table is exists");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String str : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
    }

    /**
     *
     *@Param tableName 表名
     *@Param rowKey 行键
     *@Param colFamily 列族
     *@Param col 列限定符
     *@Param val 值
     *
     **/
    public static void insertData(String tableName, String rowKey, String colFamily, String col, String val) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
        table.put(put);
        table.close();
    }

    /**
     *
     *@Param tableName 表名
     *@Param rowKey 行键
     *@Param colFamily 列族
     *@Param col 列限定符
     *
     **/
    public static void getData(String tableName, String rowKey, String colFamily, String col) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        get.addColumn(colFamily.getBytes(), col.getBytes());
        Result result = table.get(get);
        System.err.println(new String(result.getValue(colFamily.getBytes(), col == null ? null : col.getBytes())));
        table.close();
    }

}

