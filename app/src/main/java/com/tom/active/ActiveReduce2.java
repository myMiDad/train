package com.tom.active;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * ClassName: ActiveReduce2
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/23 10:29
 */
public class ActiveReduce2 extends Reducer<Text,Text,Text, NullWritable> {
    MultipleOutputs mo = null;
    Text k = null;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mo = new MultipleOutputs(context);
        k = new Text();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String flag = key.toString();
        for (Text value : values) {
            String[] split = value.toString().split(",");
            if ("all".equals(flag)){
                mo.write(value,NullWritable.get(),"all/"+split[5]+"/"+split[4]);
            }else {
                mo.write(value,NullWritable.get(),"version/"+split[3]+"/"+split[5]+"/"+split[4]);
            }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mo.close();
    }
}
