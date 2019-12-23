package com.tom.count;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: CountMapper
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/22 17:26
 */
public class CountMapper extends Mapper<LongWritable, Text,Text,LongWritable> {
    Text k;
    LongWritable v;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        k = new Text();
        v = new LongWritable(1);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取字符串
        String line = value.toString();
        String[] splits = line.split(",");
        if (splits.length<7){
            return;
        }
        //获取城市、渠道、版本
        String city = splits[6];
        String channel = splits[5];
        String version = splits[4];

        //全国、所有渠道、所有版本
        k.set("all,all,all");
        context.write(k,v);

        //全国、分渠道、所有版本
        k.set("all,"+channel+",all");
        context.write(k,v);

        //全国、所有渠道、分版本
        k.set("all,all,"+version);
        context.write(k,v);

        //全国、分渠道、分版本
        k.set("all,"+channel+","+version);
        context.write(k,v);

        //分地区、所有渠道、所有版本
        k.set(city+",all,all");
        context.write(k,v);

        //分地区、所有渠道、分版本
        k.set(city+",all,"+version);
        context.write(k,v);

        //分地区、分渠道、所有版本
        k.set(city+","+channel+",all");
        context.write(k,v);

        //分地区、分渠道、分版本
        k.set(city+","+channel+","+version);
        context.write(k,v);
    }
}
