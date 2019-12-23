package com.tom.count;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ClassName: CountReduce
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/22 22:02
 */
public class CountReduce extends Reducer<Text, LongWritable,CountBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        String[] split = key.toString().split(",", -1);
        if (split.length<3){
            return;
        }
        //创建一个容器存放对象
        Long sum = 0L;
        //遍历循环
        for (LongWritable v:values){
            sum += v.get();
        }
        //获取city、Channel、version
        String city = split[0];
        String channel = split[1];
        String version = split[2];

        //写出
        CountBean countBean = new CountBean();
        countBean.setCity(city);
        countBean.setChannel(channel);
        countBean.setVersion(version);
        countBean.setCount(sum);
        context.write(countBean,NullWritable.get());
    }
}
