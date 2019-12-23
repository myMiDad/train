package com.tom.active.count;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//分版本
public class countVerMap extends Mapper<LongWritable, Text,Text,LongWritable> {
    Text k;
    LongWritable v = new LongWritable(1);
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
       k = new Text();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取原始数据
        String line = value.toString();
        //切分
        String[] infos = line.split(",", -1);
        if (infos.length<6){
            return;
        }
        //获取版本
        String version = infos[3];
        //获取城市
        String city = infos[5];
        //获取渠道
        String channel = infos[4];

        //全部城市、全部渠道
        k.set(version+",all,all");
        context.write(k,v);

        //具体城市、全部渠道
        k.set(version+","+city+",all");
        context.write(k,v);

        //全部城市，具体渠道
        k.set(version+",all,"+channel);
        context.write(k,v);

        //具体城市，具体渠道
        k.set(version+","+city+","+channel);
        context.write(k,v);
    }
}
