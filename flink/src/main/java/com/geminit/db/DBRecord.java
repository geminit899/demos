package com.geminit.db;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBRecord implements Writable, DBWritable, Configurable {
    String record;
    private Configuration conf;

    public DBRecord() {
    }

    public DBRecord(String record) {
        this.record = record;
    }

    public void readFields(DataInput in) throws IOException {
        // no-op, since we may never need to support a scenario where you read a DBRecord from a non-RDBMS source
    }

    public void readFields(ResultSet resultSet) throws SQLException {
        record = "hahahahhaha";
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(record);
    }

    public void write(PreparedStatement stmt) throws SQLException {
        stmt.setString(1, record);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
