package com.tom.active;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * ClassName: ActiveDriver2
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/23 10:18
 */
public class ActiveDriver2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(ActiveDriver2.class);

        job.setMapperClass(ActiveMapper2.class);
        job.setReducerClass(ActiveReduce2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileInputFormat.setInputDirRecursive(job,true);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
