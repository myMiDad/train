package com.tom.active;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * ClassName: ActiveReduce1
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/23 9:24
 */
public class ActiveReduce1 extends Reducer<Text, Text,Text, NullWritable> {
    private Text k = null;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        k=new Text();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        TreeMap<Long, String> map = new TreeMap<>();
        for (Text value : values) {
            String[] split = value.toString().split(",",-1);
            String commit_time = split[6];
            if (StringUtils.isNotEmpty(commit_time)){
                map.put(Long.parseLong(commit_time),split[0]+","+split[1]+","+split[2]+","+split[3]+","+split[4]+","+split[5]);
            }
        }
        Map.Entry<Long, String> entrty = map.firstEntry();
        String[] arr = entrty.getValue().split(",");

        k.set(entrty.getValue());
        context.write(k,NullWritable.get());
    }
}
