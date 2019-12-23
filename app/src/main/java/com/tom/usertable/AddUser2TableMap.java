package com.tom.usertable;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * ClassName: AddUser2Table
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/22 9:32
 */
public class AddUser2TableMap extends Mapper<LongWritable, Text,Text,Text> {
    private String day ="";
    private Text keyOut=null;
    private Text valueOut=null;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        day = context.getConfiguration().get("date");
        keyOut = new Text();
        valueOut = new Text();
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获得原始数据
        String line = value.toString();
        //将原始数据转换成JSON
        JSONObject jsonObject = JSON.parseObject(line);
        JSONObject header = jsonObject.getJSONObject("header");

        String user_id = header.getString("user_id");
        String app_token = header.getString("app_token");
        String commit_time = header.getString("commit_time");
        String app_ver_name = header.getString("app_ver_name");
        String release_channel = header.getString("release_channel");
        String city = header.getString("city");

        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");

        String k = app_token+"|"+user_id;
        keyOut.set(k);
        valueOut.set(day+","+app_token+","+user_id+","+commit_time+","+app_ver_name+","+release_channel+","+city);
        context.write(keyOut,valueOut);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
