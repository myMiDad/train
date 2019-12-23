package com.tom.active;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: ActiveMapper2
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/23 10:18
 */
public class ActiveMapper2 extends Mapper<LongWritable, Text,Text,Text> {
    private Text k = null;
    private Text v = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        k = new Text();
        v = new Text();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取字符串
        String line = value.toString();
        String[] splits = line.split(",");
        if (splits.length<6){
            return;
        }
        String day = splits[0];
        String app_token = splits[1];
        String user_id = splits[2];
        String version = splits[3];
        String channel = splits[4];
        String city = splits[5];
        v.set(line);
        k.set("all");
        context.write(k,v);
        k.set("notAll");
        context.write(k,v);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
