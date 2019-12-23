package com.tom.usertable;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * ClassName: AddUser2TableReduce
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/22 10:23
 */
public class AddUser2TableReduce extends Reducer<Text,Text,Text, NullWritable> {
    private Text k;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        k = new Text();
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        TreeMap<Long, String> treeMap = new TreeMap<>();

        for (Text value:values){
            String line = value.toString();
            String[] splits = line.split(",",-1);
            String commit_time = splits[3];
            treeMap.put(Long.parseLong(commit_time),line);
        }
        Map.Entry<Long, String> entry = treeMap.firstEntry();
        System.out.println(entry.getKey()+"------"+entry.getValue());

        k.set(entry.getValue());

        context.write(k,NullWritable.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
