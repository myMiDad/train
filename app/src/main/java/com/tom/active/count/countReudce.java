package com.tom.active.count;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class countReudce extends Reducer<Text, LongWritable,Text,LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //创建一个容器来存放个数
        Long sum = 0L;
        //遍历循环values
        for (LongWritable value : values) {
            sum ++;
        }
        //写出
        context.write(key,new LongWritable(sum));
    }
}
