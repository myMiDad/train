package com.tom.count;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * ClassName: CountBean
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/22 22:03
 */
public class CountBean implements Writable, DBWritable {
    //定义成员变量
    private String city;
    private String channel;
    private String version;
    private Long count;

    //构造器
    public CountBean() {
    }

    public CountBean(String city, String channel, String version, Long count) {
        this.city = city;
        this.channel = channel;
        this.version = version;
        this.count = count;
    }

    //写出文本序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(city);
        out.writeUTF(channel);
        out.writeUTF(version);
        out.writeLong(count);
    }

    //读取数据反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.city = in.readUTF();
        this.channel = in.readUTF();
        this.version = in.readUTF();
        this.count = in.readLong();
    }

    //写入数据库反序列化
    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1,city);
        preparedStatement.setString(2,channel);
        preparedStatement.setString(3,version);
        preparedStatement.setLong(4,count);
    }

    //读取数据库反序列化
    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.city = resultSet.getString(1);
        this.channel = resultSet.getString(2);
        this.version = resultSet.getString(3);
        this.count = resultSet.getLong(4);
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "CountBean{" +
                "city='" + city + '\'' +
                ", channel='" + channel + '\'' +
                ", version='" + version + '\'' +
                ", count=" + count +
                '}';
    }
}
