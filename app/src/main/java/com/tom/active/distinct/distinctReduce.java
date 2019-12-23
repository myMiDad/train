package com.tom.active.distinct;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class distinctReduce extends Reducer<Text, Text, Text, NullWritable> {
    MultipleOutputs<Text,NullWritable> mo;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mo = new MultipleOutputs<Text,NullWritable>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //获取value中的一个值
        Text next = values.iterator().next();
        //获取key
        String keyStr = key.toString();
        //切分
        String[] keys = keyStr.split("\\|", -1);
        //判断keys长度是一个还是两个
        if (keys.length == 3){
            mo.write(next,NullWritable.get(),"allversion/");
        }else{
            mo.write(next,NullWritable.get(),"concreteversion/");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mo.close();
    }
}
