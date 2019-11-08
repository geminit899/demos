package com.geminit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by root on 9/23/19.
 */
public class DBUtils {
    private static final String url = "jdbc:mysql://192.168.0.127:3306/htsc?useUnicode=true&amp;characterEncoding=utf8&amp;autoReconnect=true";
    private static final String username = "root";
    private static final String password = "hack";

    private static String sql = "select #{username},#{sex},#{class} from ad_t_entity;";
    private static Map<String, String> map = new HashMap<>();

    static {
        map.put("username", "ad_id");
        map.put("sex", "ad_name");
        map.put("class", "ad_addtime");
    }

    public static void prepareRun() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection(String url, String username, String password) {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static ResultSet executeQuery(Connection conn, String sql) {
        ResultSet resultSet = null;
        try {
            PreparedStatement st = conn.prepareStatement(sql);
            resultSet = st.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSet;
    }

    public static String rebuildSQL(String sql) {
        Pattern pattern = Pattern.compile("#\\{(.*?)\\}");
        Matcher matcher = pattern.matcher(sql);

        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            matcher.appendReplacement(sb, map.get(matcher.group(1)));
        }
        matcher.appendTail(sb);

        return sb.toString();
    }

    public static void main(String[] args) {
        prepareRun();

        Connection conn = getConnection(url, username, password);

        sql = rebuildSQL(sql);

        ResultSet resultSet = executeQuery(conn, sql);

        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            List<Map<String, String>> result = new ArrayList<>();
            while (resultSet.next()) {
                Map<String, String> resultMap = new HashMap<>();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    resultMap.put(resultSetMetaData.getColumnName(i), resultSet.getString(i));
                }
                result.add(resultMap);
            }
            System.out.println();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
