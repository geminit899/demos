package com.geminit.rwcompute;

import org.apache.commons.lang.time.DateUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

public class RegionalWarningCompute {
    private static final Logger LOG = LoggerFactory.getLogger(RegionalWarningCompute.class);

    private static final String POSTGRESQL_DRIVER = "org.postgresql.Driver";
    private static final String POSTGRESQL_USER = "postgres";
    private static final String POSTGRESQL_PASS = "root";
    private static final String POSTGRESQL_URL = "jdbc:postgresql://localhost:5432/test";

    private static SimpleDateFormat hmFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private static SimpleDateFormat hmsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static Properties props = new Properties();
    private static Connection connection;

    static {
        try {
            Class.forName(POSTGRESQL_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            connection = DriverManager.getConnection(POSTGRESQL_URL, POSTGRESQL_USER, POSTGRESQL_PASS);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.71:6667");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 8388608);  //8M
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);  //64M
    }

    public static void main(String[] args) {
        while (true) {
            String time = hmFormat.format(new Date());
            String sql = "SELECT id,indexDate,eventId,dims,spaces,batch,maxNum " +
                    "from rwCompute where indexDate = '" + time + ":00'";
            ResultSet resultSet = null;
            try {
                resultSet = connection.prepareStatement(sql).executeQuery();
            } catch (SQLException e) {
                e.printStackTrace();
                LOG.error("Time : " + time + " get data from rwCompute failed.");
                continue;
            }

            try {
                computeResult(resultSet);
            } catch (SQLException e) {
                e.printStackTrace();
                LOG.error("Exception occurred in computeResult(). In resultSet.next() or getValue from resultSet.");
            }

            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                LOG.info("Thread.sleep(10000 ms) failed.");
            }
        }
    }

    public static void computeResult(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
            String id = String.valueOf(resultSet.getInt("id"));
            Date indexDate = resultSet.getDate("indexDate");
            String eventId = resultSet.getString("eventId");
            List<String> dims = Arrays.asList(resultSet.getString("dims").split(","));
            List<String> spaces = Arrays.asList(resultSet.getString("spaces").split(","));
            int batch = resultSet.getInt("batch");
            int maxNum = resultSet.getInt("maxNum");

            String fromDate = hmsFormat.format(DateUtils.addMinutes(indexDate, -batch));
            String toDate = hmsFormat.format(indexDate);

            spaces.forEach(new Consumer<String>() {
                @Override
                public void accept(String space) {
                    JSONArray jsonArray = new JSONArray();
                    dims.forEach(new Consumer<String>() {
                        @Override
                        public void accept(String dim) {
                            String keyFieldName = getKeyFieldNameByDim(dim);
                            String sql = getSqlByDimension(dim, fromDate, toDate, space);
                            try {
                                PreparedStatement pre = connection.prepareStatement(sql);
                                ResultSet rs = pre.executeQuery();

                                while (rs.next()) {
                                    String key = rs.getString(keyFieldName);
                                    Object timesObject = rs.getArray("time").getArray();
                                    Timestamp[] times = (Timestamp[]) timesObject;

                                    JSONObject jsonObject = new JSONObject();
                                    jsonObject.put("type", dim);
                                    jsonObject.put("value", key);
                                    jsonObject.put("capTime", times[0].toString());
                                    jsonArray.put(jsonObject);
                                }
                            } catch (SQLException e) {
                                e.printStackTrace();
                                LOG.error("Exception has occurred when performing SQL : " + sql);
                            } catch (JSONException e) {
                                e.printStackTrace();
                                LOG.error("Exception has occurred about JSON.");
                            }
                        }
                    });
                    if (jsonArray.length() < maxNum) {
                        return;
                    }
                    String resultStr = eventId + "__" + hmsFormat.format(new Date()) + "__" +
                            space + "__" + maxNum + "__" + jsonArray.toString();
                    sendToKafka(resultStr);
                }
            });
            try {
                connection.createStatement().executeQuery("DELETE FROM rwCompute WHERE id = '" + id + "'");
            } catch (SQLException e) {
                e.printStackTrace();
                LOG.error("Delete the record that has been handled failed. The record's id in rwCompute is " + id);
            }
        }
    }

    public static void sendToKafka(String messageStr) {
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);

        String topic = "rwResult";
        int messageNo = 1;

        try {
            producer.send(new ProducerRecord<Integer, String>(topic, messageNo, messageStr)).get();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Exception has occurred when sending message to kafka.");
        }
        producer.close();
    }

    public static String getKeyFieldNameByDim(String dim) {
        String keyFieldName = "";
        switch (dim) {
            case "imsi":
                keyFieldName = "imsi";
                break;
            case "face":
                keyFieldName = "face_id";
                break;
            case "car":
                keyFieldName = "plate_number";
                break;
        }
        return keyFieldName;
    }

    public static String getSqlByDimension(String dimension, String fromDate, String toDate, String space) {
        String keyFieldName = getKeyFieldNameByDim(dimension);
        String capTimeFieldName = "";
        String relationCodeFieldName = "";
        String tableName = "";
        switch (dimension) {
            case "imsi":
                capTimeFieldName = "cap_time";
                relationCodeFieldName = "relation_code";
                tableName = "lte_data";
                break;
            case "face":
                capTimeFieldName = "cap_time";
                relationCodeFieldName = "relation_code";
                tableName = "face";
                break;
            case "car":
                capTimeFieldName = "cap_time";
                relationCodeFieldName = "relation_code";
                tableName = "passing_vehicle_data";
                break;
        }

        StringBuilder script = new StringBuilder();
        script.append("select ").append(keyFieldName).append(",");
        script.append("array_agg(").append(capTimeFieldName).append(") as time");
        script.append(" from public.").append(tableName);
        script.append(" where ").append(capTimeFieldName).append(" between '");
        script.append(fromDate).append("' and '").append(toDate).append("' AND ");
        script.append(relationCodeFieldName).append("=").append(space);
        script.append(" group by ").append(keyFieldName);

        return script.toString();
    }
}
