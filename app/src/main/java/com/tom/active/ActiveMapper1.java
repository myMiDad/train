package com.tom.active;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: ActiveMapper
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/23 8:51
 */
public class ActiveMapper1 extends Mapper<LongWritable, Text, Text, Text> {
    Text keyOut = null;
    Text valueOut = null;
    String day = "";
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        keyOut = new Text();
        valueOut = new Text();
        day = context.getConfiguration().get("date");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //原始数据
        String line = value.toString();
        //将原始数据转换为json
        JSONObject jsonObject = JSON.parseObject(line);
        JSONObject header = jsonObject.getJSONObject("header");
        //获取app_token、user_id、version、channel、city
        String app_token = header.getString("app_token");
        String user_id = header.getString("user_id");
        String version = header.getString("app_ver_name");
        String channel = header.getString("release_channel");
        String city = header.getString("city");
        String commit_time = header.getString("commit_time");

        //写出到reduce
        keyOut.set(app_token+"|"+user_id);
        valueOut.set(day+","+app_token+","+user_id+","+version+","+channel+","+city+","+commit_time);
        context.write(keyOut,valueOut);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
