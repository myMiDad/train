package com.tom.active;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: ActiveMapper
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/23 8:51
 */
public class ActiveMapper extends Mapper {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
