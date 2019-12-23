package com.tom.count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * ClassName: CountDriver
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/22 22:18
 */
public class CountDriver {

    private static String driver = "com.mysql.jdbc.Driver";
    private static String url = "jdbc:mysql://10.10.51.235:3306/appdb?characterEncoding=utf8";
    private static String user = "root";
    private static String password = "root";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        DBConfiguration.configureDB(conf,driver,url,user,password);
        //job
        Job job = Job.getInstance(conf);
        //jar
        job.setJarByClass(CountDriver.class);

        //设置mapper、reduce类
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReduce.class);

        //设置Map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置最后的输出类型
        job.setOutputKeyClass(CountBean.class);
        job.setOutputValueClass(NullWritable.class);

        //设置输入
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileInputFormat.setInputDirRecursive(job,true);
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //输出到数据库
        DBOutputFormat.setOutput(job,"count_table","city","channel","version","sum");

        //执行
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
