package com.tom.active.distinct;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.awt.*;
import java.io.IOException;

public class distinctMap extends Mapper<LongWritable, Text, Text, Text> {
    String day;

    Text v;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        day = context.getConfiguration().get("date");
        v = new Text();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取原始日志
        String line = value.toString();
        //json解析
        JSONObject jsonObject = JSON.parseObject(line);
        //获取header
        JSONObject header = jsonObject.getJSONObject("header");
        //app_token
        String app_token = header.getString("app_token");
        // user_id
        String user_id = header.getString("user_id");
        //version
        String version = header.getString("app_ver_name");
        //channel
        String channel = header.getString("release_channel");
        //city
        String city = header.getString("city");

        v.set(day + "," + app_token + "," + user_id + "," + version + "," + channel + "," + city);

        //写不分版本的
        Text allKey = new Text(app_token + "|" + user_id);
        context.write(allKey, v);

        //写具体版本的
        Text verKey = new Text(app_token + "|" + user_id + "|" + version);
        context.write(verKey, v);
    }
}
