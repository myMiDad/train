package com.tom.usertable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * ClassName: AddUser2TableDriver
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/22 9:40
 */
public class AddUser2TableDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("date",args[0]);
        Job job = Job.getInstance(conf);

        job.setJarByClass(AddUser2TableDriver.class);
        job.setMapperClass(AddUser2TableMap.class);
        job.setReducerClass(AddUser2TableReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path(args[1]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));

        boolean b = job.waitForCompletion(true);

        System.out.println(b);
        System.exit(b ? 0:1);

    }
}
