package com.tom.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * ClassName: Json2JsonDriver
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/21 17:13
 */
public class Json2JsonDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length < 2) {
            System.out.println("参数异常");
            return;
        }
        //conf
        Configuration conf = new Configuration();
        //job
        Job job = Job.getInstance(conf);
        job.setJarByClass(Json2JsonDriver.class);
        //设置Map类
        job.setMapperClass(Json2JsonMap.class);

        //设置Map最后输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置reduce个数
        job.setNumReduceTasks(0);
        //设置最后输出的key和value类型
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //启动程序
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0:1);

    }
}
