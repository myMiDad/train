package com.tom.active.distinct;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class distinctDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //conf
        Configuration conf = new Configuration();
        //date
        conf.set("date", "20191223");
        //job
        Job job = Job.getInstance(conf);
        //jar
        job.setJarByClass(distinctDriver.class);

        //map和reduce 类
        job.setMapperClass(distinctMap.class);
        job.setReducerClass(distinctReduce.class);

        //map的输出类
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置最后的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //判断输入路径
        Path inpath = new Path(args[0]);
        //获取输入文件系统
        FileSystem fs = FileSystem.get(conf);
        if (fs.isDirectory(inpath)) {
            //查找里面所有的文件系统
            FileStatus[] fss = fs.listStatus(inpath);
            //遍历判断
            for (FileStatus fileStatus : fss) {
                if (fs.isDirectory(fileStatus.getPath())) {
                    //设置输入和输出的文件
                    FileInputFormat.addInputPath(job, fileStatus.getPath());
                }
            }
        }

        //输出文件系统
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交进程
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
