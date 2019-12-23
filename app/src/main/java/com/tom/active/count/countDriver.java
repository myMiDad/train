package com.tom.active.count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class countDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //conf
        Configuration conf = new Configuration();
        //job
        Job job = Job.getInstance(conf);
        //jar
        job.setJarByClass(countDriver.class);

        //设置map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置最后的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置输入和map的类
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, countMap.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, countVerMap.class);

        //设置reduce
        job.setReducerClass(countReudce.class);

        //设置输出的文件
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        //提交程序
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
