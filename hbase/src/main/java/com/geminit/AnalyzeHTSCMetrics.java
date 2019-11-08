package com.geminit;

import org.apache.commons.httpclient.util.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

/**
 * Created by tyx on 10/16/19.
 */
public class AnalyzeHTSCMetrics {
    public static Configuration configuration; //管理Hbase的配置信息
    public static Connection connection; //管理Hbase连接
    public static Admin admin; //管理Hbase数据库的表信息

    public static Map<String, Map<Long, Set<String>>> messageTableKeyTimeValues = new HashMap<>();
    public static Map<String, Map<Long, Set<String>>> messageTableValueTimeKeys = new HashMap<>();

    public static Map<String, Map<Long, Set<String>>> entityTableKeyTimeValues = new HashMap<>();
    public static Map<String, Map<Long, Set<String>>> entityTableValueTimeKeys = new HashMap<>();
    public static Map<String, Map<Long, Set<String>>> metaTableKeyTimeValues = new HashMap<>();
    public static Map<String, Map<Long, Set<String>>> metaTableValueTimeKeys = new HashMap<>();
    public static Map<String, Map<Long, Set<String>>> ts1TableKeyTimeValues = new HashMap<>();
    public static Map<String, Map<Long, Set<String>>> ts1TableValueTimeKeys = new HashMap<>();
    public static Map<String, Map<Long, Set<String>>> ts60TableKeyTimeValues = new HashMap<>();
    public static Map<String, Map<Long, Set<String>>> ts60TableValueTimeKeys = new HashMap<>();
    public static Map<String, Map<Long, Set<String>>> ts3600TableKeyTimeValues = new HashMap<>();
    public static Map<String, Map<Long, Set<String>>> ts3600TableValueTimeKeys = new HashMap<>();
    public static Map<String, Map<Long, Set<String>>> ts2147483647TableKeyTimeValues = new HashMap<>();
    public static Map<String, Map<Long, Set<String>>> ts2147483647TableValueTimeKeys = new HashMap<>();

    public static void init() throws FileNotFoundException {
        configuration = HBaseConfiguration.create();
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

    public static void getHbaseData() throws Exception {
        String messageTable = "cdap_system:tms.message";
        messageTableKeyTimeValues = getKeyTimeValues(messageTable);
        messageTableValueTimeKeys = getValueTimeKeys(messageTable);

//        String entityTable = "cdap_system:metrics.entity";
//        entityTableKeyTimeValues = getKeyTimeValues(entityTable);
//        entityTableValueTimeKeys = getValueTimeKeys(entityTable);
//        String metaTable = "cdap_system:metrics.meta";
//        metaTableKeyTimeValues = getKeyTimeValues(metaTable);
//        metaTableValueTimeKeys = getValueTimeKeys(metaTable);
//        String ts1Table = "cdap_system:metrics.table.ts.1";
//        ts1TableKeyTimeValues = getKeyTimeValues(ts1Table);
//        ts1TableValueTimeKeys = getValueTimeKeys(ts1Table);
//        String ts60Table = "cdap_system:metrics.table.ts.1";
//        ts60TableKeyTimeValues = getKeyTimeValues(ts60Table);
//        ts60TableValueTimeKeys = getValueTimeKeys(ts60Table);
//        String ts3600Table = "cdap_system:metrics.table.ts.1";
//        ts3600TableKeyTimeValues = getKeyTimeValues(ts3600Table);
//        ts3600TableValueTimeKeys = getValueTimeKeys(ts3600Table);
//        String ts2147483647Table = "cdap_system:metrics.table.ts.2147483647";
//        ts2147483647TableKeyTimeValues = getKeyTimeValues(ts2147483647Table);
//        ts2147483647TableValueTimeKeys = getValueTimeKeys(ts2147483647Table);
    }

    public static void main(String[] args) throws Exception {
        init();

        Table table = connection.getTable(TableName.valueOf("cdap_system:tms.message"));

        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //开头大于等于starTm的行
        Filter starTime = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
                new BinaryPrefixComparator("20191022100040000".getBytes()));
        filters.addFilter(starTime);
        //开头小于等于endTime的行
        Filter endTime = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
                new BinaryPrefixComparator("20191023100300000".getBytes()));
        filters.addFilter(endTime);

        Scan scan = new Scan();
//        scan.setStartRow("startRowKey".getBytes());
//        scan.setStopRow("stopRowKey".getBytes());
//        scan.setFilter(filters);

        Map<String, Map<Long, Set<String>>> keyTimeValues = new HashMap<>();
        ResultScanner scanner = table.getScanner(scan);
        for(Result r : scanner){
            String rowKey = Bytes.toString(r.getRow());
            if (!rowKey.contains("system:metrics")) {
                continue;
            }
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map1 = r.getMap();
            for(byte[] family : map1.keySet()){
                NavigableMap<byte[], NavigableMap<Long, byte[]>> map2 = map1.get(family);
                for (byte[] qualifer : map2.keySet()) {
                    NavigableMap<Long, byte[]> map3 = map2.get(qualifer);
                    for (Long timeStamp : map3.keySet()) {
                        String value = Bytes.toString(map3.get(timeStamp));

                        if (!value.contains("records.out") && !value.contains("records.in")) {
                            continue;
                        }

                        if (!keyTimeValues.containsKey(rowKey)) {
                            Map<Long, Set<String>> timeValue = new HashMap<>();
                            keyTimeValues.put(rowKey, timeValue);
                        }
                        if (keyTimeValues.get(rowKey).containsKey(timeStamp)) {
                            keyTimeValues.get(rowKey).get(timeStamp).add(value);
                        } else {
                            Set<String> values = new HashSet<>();
                            values.add(value);
                            keyTimeValues.get(rowKey).put(timeStamp, values);
                        }
                    }
                }
            }
        }


//        getHbaseData();

//        String sparkRunId = "9d1c5e22-e358-11e9-a2cf-bab5ba5dfe55";
//        String flinkRunId = "ca6ab2be-eef1-11e9-aa79-c63723931316";
//        String randomFlinkRunId = "ecf39c84-efb8-11e9-8b0b-c63723931316";
//
//        Map<String, List<String>> info1 = searchRunId(sparkRunId);
//        Map<String, List<String>> info2 = searchRunId(flinkRunId);
//        Map<String, List<String>> info3 = searchRunId(randomFlinkRunId);
//
//        Map<String, Map<String, List<String>>> allMap = searchAllRunId();

        close();
    }

    public static Map<String, Map<String, List<String>>> searchAllRunId() {
        Map<String, Map<String, List<String>>> allMap = new HashMap<>();
        for (String str : entityTableKeyTimeValues.keySet()) {
            if (str.contains("-")) {
                Map<Long, Set<String>> timeValues = entityTableKeyTimeValues.get(str);
                for (Long time : timeValues.keySet()) {
                    Set<String> values = timeValues.get(time);
                    for (String value : values) {
                        Map<String, List<String>> info = searchId(value);
                        if (info.get("ts1Table").size() > 0) {
                            allMap.put(str, info);
                        }
                    }
                }
            }
        }
        return allMap;
    }

    //根据模型的runId，在entity表中获取一个id，然后根据这个id去entity和ts表中获取相关内容。
    public static Map<String, List<String>> searchRunId(String runId) {
        String id = null;
        for (String str : entityTableKeyTimeValues.keySet()) {
            if (str.contains(runId)) {
                Map<Long, Set<String>> timeValues = entityTableKeyTimeValues.get(str);
                for (Long time : timeValues.keySet()) {
                    Set<String> values = timeValues.get(time);
                    for (String value : values) {
                        id = value;
                    }
                }
            }
        }
        if (id == null) {
            return null;
        }
        return searchId(id);
    }

    //根据16进制的字符串，去ts表中获取相关内容
    public static Map<String, List<String>> searchId(String id) {
        Map<String, List<String>> info = new HashMap<>();
        List<String> entityInfo = new ArrayList<>();
        for (String str : entityTableKeyTimeValues.keySet()) {
            if (str.contains(id)) {
                Map<Long, Set<String>> timeValues = entityTableKeyTimeValues.get(str);
                for (Long time : timeValues.keySet()) {
                    entityInfo.addAll(timeValues.get(time));
                }
            }
        }
        info.put("entityTable", entityInfo);

        List<String> ts1Info = new ArrayList<>();
        for (String str : ts1TableKeyTimeValues.keySet()) {
            if (str.contains(id)) {
                Map<Long, Set<String>> timeValues = ts1TableKeyTimeValues.get(str);
                for (Long time : timeValues.keySet()) {
                    ts1Info.addAll(timeValues.get(time));
                }
            }
        }
        info.put("ts1Table", ts1Info);
        List<String> ts60Info = new ArrayList<>();
        for (String str : ts60TableKeyTimeValues.keySet()) {
            if (str.contains(id)) {
                Map<Long, Set<String>> timeValues = ts60TableKeyTimeValues.get(str);
                for (Long time : timeValues.keySet()) {
                    ts60Info.addAll(timeValues.get(time));
                }
            }
        }
        info.put("ts60Table", ts60Info);
        List<String> ts3600Info = new ArrayList<>();
        for (String str : ts3600TableKeyTimeValues.keySet()) {
            if (str.contains(id)) {
                Map<Long, Set<String>> timeValues = ts3600TableKeyTimeValues.get(str);
                for (Long time : timeValues.keySet()) {
                    ts3600Info.addAll(timeValues.get(time));
                }
            }
        }
        info.put("ts3600Table", ts3600Info);
        List<String> ts2147483647Info = new ArrayList<>();
        for (String str : ts2147483647TableKeyTimeValues.keySet()) {
            if (str.contains(id)) {
                Map<Long, Set<String>> timeValues = ts2147483647TableKeyTimeValues.get(str);
                for (Long time : timeValues.keySet()) {
                    ts2147483647Info.addAll(timeValues.get(time));
                }
            }
        }
        info.put("ts2147483647Table", ts2147483647Info);

        return info;
    }

    //获取最新时间戳
    public static String getLatestTime(Map<Long, Set<String>> timeKeys) {
        List<Long> time = new ArrayList<>(timeKeys.keySet());
        Collections.sort(time);
        char[] s = String.valueOf(time.get(time.size() - 1)).toCharArray();
        Long latestTime = Long.parseLong(new String(Arrays.copyOfRange(s, 0, 13)));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date(latestTime));
    }

    //getKeyTimeValues，每个rowKey在各个时间的所有值
    //key：hbase中的rowKey；time：时间戳；value：值
    public static Map<String, Map<Long, Set<String>>> getKeyTimeValues(String tableName) throws Exception {
        Map<String, Map<Long, Set<String>>> keyTimeValues = new HashMap<>();

        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for(Result r : scanner){
            String rowKey = Bytes.toString(r.getRow());
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map1 = r.getMap();
            for(byte[] family : map1.keySet()){
                NavigableMap<byte[], NavigableMap<Long, byte[]>> map2 = map1.get(family);
                for (byte[] qualifer : map2.keySet()) {
                    NavigableMap<Long, byte[]> map3 = map2.get(qualifer);
                    for (Long timeStamp : map3.keySet()) {
                        String value = Bytes.toString(map3.get(timeStamp));

                        if (!keyTimeValues.containsKey(rowKey)) {
                            Map<Long, Set<String>> timeValue = new HashMap<>();
                            keyTimeValues.put(rowKey, timeValue);
                        }
                        if (keyTimeValues.get(rowKey).containsKey(timeStamp)) {
                            keyTimeValues.get(rowKey).get(timeStamp).add(value);
                        } else {
                            Set<String> values = new HashSet<>();
                            values.add(value);
                            keyTimeValues.get(rowKey).put(timeStamp, values);
                        }
                    }
                }
            }
        }

        return keyTimeValues;
    }

    //每个值在各个时间对应的所有rowKey
    public static Map<String, Map<Long, Set<String>>> getValueTimeKeys(String tableName) throws Exception {
        Map<String, Map<Long, Set<String>>> valueTimeKeys = new HashMap<>();


        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for(Result r : scanner){
            String rowKey = Bytes.toString(r.getRow());
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map1 = r.getMap();
            for(byte[] family : map1.keySet()){
                NavigableMap<byte[], NavigableMap<Long, byte[]>> map2 = map1.get(family);
                for (byte[] qualifer : map2.keySet()) {
                    NavigableMap<Long, byte[]> map3 = map2.get(qualifer);
                    for (Long timeStamp : map3.keySet()) {
                        String value = Bytes.toString(map3.get(timeStamp));

                        if (!valueTimeKeys.containsKey(value)) {
                            Map<Long, Set<String>> timeKeys = new HashMap<>();
                            valueTimeKeys.put(value, timeKeys);
                        }
                        if (valueTimeKeys.get(value).containsKey(timeStamp)) {
                            valueTimeKeys.get(value).get(timeStamp).add(rowKey);
                        } else {
                            Set<String> keys = new HashSet<>();
                            keys.add(rowKey);
                            valueTimeKeys.get(value).put(timeStamp, keys);
                        }
                    }
                }
            }
        }

        return valueTimeKeys;
    }
}
