package com.geminit.db;

import com.google.common.base.Throwables;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class ETLDBOutputFormat<K extends DBWritable, V>  extends DBOutputFormat<K, V> {
    private String user = "root";
    private String password = "ht1234";
    private String connectionURL = "jdbc:mysql://192.168.0.114:3306/demo?useUnicode=true&characterEncoding=utf8&autoReconnect=true";
    private String tableName = "test";
    private String[] fieldNames = new String[]{"message"};

    private Driver driver;
    private JDBCDriverShim driverShim;

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException {
        try {
            Connection connection = getConnection();
            PreparedStatement statement = connection.prepareStatement(constructQuery(tableName, fieldNames));
            return new DBRecordWriter(connection, statement) {

                private boolean emptyData = true;

                //Implementation of the close method below is the exact implementation in DBOutputFormat except that
                //we check if there is any data to be written and if not, we skip executeBatch call.
                //There might be reducers that don't receive any data and thus this check is necessary to prevent
                //empty data to be committed (since some Databases doesn't support that).
                @Override
                public void close(TaskAttemptContext context) throws IOException {
                    try {
                        if (!emptyData) {
                            getStatement().executeBatch();
                            getConnection().commit();
                        }
                    } catch (SQLException e) {
                        try {
                            getConnection().rollback();
                        } catch (SQLException ex) {
                            System.out.println(StringUtils.stringifyException(ex));
                        }
                        throw new IOException(e);
                    } finally {
                        try {
                            getStatement().close();
                            getConnection().close();
                        } catch (SQLException ex) {
                            throw new IOException(ex);
                        }
                    }

                    try {
                        DriverManager.deregisterDriver(driverShim);
                    } catch (SQLException e) {
                        throw new IOException(e);
                    }
                }

                @Override
                public void write(K key, V value) throws IOException {
                    super.write(key, value);
                    emptyData = false;
                }
            };
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    private Connection getConnection() {
        Connection connection;
        try {
            String url = connectionURL;
            try {
                // throws SQLException if no suitable driver is found
                DriverManager.getDriver(url);
            } catch (SQLException e) {
                if (driverShim == null) {
                    if (driver == null) {
                        @SuppressWarnings("unchecked")
                        Class<? extends Driver> driverClass = (Class<? extends Driver>) this.getClass().getClassLoader().loadClass("com.mysql.jdbc.Driver");
                        driver = driverClass.newInstance();

                        // De-register the default driver that gets registered when driver class is loaded.
                        deregisterAllDrivers(driverClass);
                    }

                    driverShim = new JDBCDriverShim(driver);
                    DriverManager.registerDriver(driverShim);
                }
            }

            Properties properties = new Properties();
            properties.put("user", user);
            properties.put("password", password);
            connection = DriverManager.getConnection(url, properties);

            connection.setAutoCommit(false);
            connection.setTransactionIsolation(8);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return connection;
    }

    @Override
    public String constructQuery(String table, String[] fieldNames) {
        String query = super.constructQuery(table, fieldNames);
        // Strip the ';' at the end since Oracle doesn't like it.
        // TODO: Perhaps do a conditional if we can find a way to tell that this is going to Oracle
        // However, tested this to work on Mysql and Oracle
        query = query.substring(0, query.length() - 1);

        return query;
    }

    public void deregisterAllDrivers(Class<? extends Driver> driverClass)
            throws NoSuchFieldException, IllegalAccessException, ClassNotFoundException {
        Field field = DriverManager.class.getDeclaredField("registeredDrivers");
        field.setAccessible(true);
        List<?> list = (List<?>) field.get(null);
        for (Object driverInfo : list) {
            Class<?> driverInfoClass = this.getClass().getClassLoader().loadClass("java.sql.DriverInfo");
            Field driverField = driverInfoClass.getDeclaredField("driver");
            driverField.setAccessible(true);
            Driver d = (Driver) driverField.get(driverInfo);
            if (d == null) {
                continue;
            }
            ClassLoader registeredDriverClassLoader = d.getClass().getClassLoader();
            if (registeredDriverClassLoader == null) {
                continue;
            }
            // Remove all objects in this list that were created using the classloader of the caller.
            if (d.getClass().getClassLoader().equals(driverClass.getClassLoader())) {
                list.remove(driverInfo);
            }
        }
    }
}
