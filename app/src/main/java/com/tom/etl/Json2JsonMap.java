package com.tom.etl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * ClassName: Json2JsonMap
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/21 16:07
 */
public class Json2JsonMap extends Mapper<LongWritable, Text, Text, NullWritable> {

    MultipleOutputs<Text, NullWritable> mo = null;

    Text k = null;
    //初始化资源
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mo = new MultipleOutputs<Text, NullWritable>(context);
        k = new Text();
    }

    //主业务逻辑
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取原始数据
        String line = value.toString();
        //转成json数据
        JSONObject json = JSON.parseObject(line);
        JSONObject header = json.getJSONObject("header");


        String sdk_ver = header.getString("sdk_ver");
        if (StringUtils.isEmpty(sdk_ver)) {
            return;
        }

        String time_zone = header.getString("time_zone");
        if (StringUtils.isEmpty(time_zone)) {
            return;
        }
        String commit_id = header.getString("commit_id");
        if (StringUtils.isEmpty(commit_id)) {
            return;
        }
        String commit_time = header.getString("commit_time");
        if (StringUtils.isEmpty(commit_time)) {
            return;
        }
        String pid = header.getString("pid");
        if (StringUtils.isEmpty(pid)) {
            return;
        }
        String app_token = header.getString("app_token");
        if (StringUtils.isEmpty(app_token)) {
            return;
        }
        String app_id = header.getString("app_id");
        if (StringUtils.isEmpty(app_id)) {
            return;
        }
        String device_id = header.getString("device_id");
        if (StringUtils.isEmpty(device_id)) {
            return;
        }
        String device_id_type = header.getString("device_id_type");
        if (StringUtils.isEmpty(device_id_type)) {
            return;
        }
        String release_channel = header.getString("release_channel");
        if (StringUtils.isEmpty(release_channel)) {
            return;
        }
        String app_ver_name = header.getString("app_ver_name");
        if (StringUtils.isEmpty(app_ver_name)) {
            return;
        }
        String app_ver_code = header.getString("app_ver_code");
        if (StringUtils.isEmpty(app_ver_code)) {
            return;
        }
        String os_name = header.getString("os_name");
        if (StringUtils.isEmpty(os_name)) {
            return;
        }
        String os_ver = header.getString("os_ver");
        if (StringUtils.isEmpty(os_ver)) {
            return;
        }
        String language = header.getString("language");
        if (StringUtils.isEmpty(language)) {
            return;
        }
        String country = header.getString("country");
        if (StringUtils.isEmpty(country)) {
            return;
        }
        String manufacture = header.getString("manufacture");
        if (StringUtils.isEmpty(manufacture)) {
            return;
        }
        String device_model = header.getString("device_model");
        if (StringUtils.isEmpty(device_model)) {
            return;
        }
        String resolution = header.getString("resolution");
        if (StringUtils.isEmpty(resolution)) {
            return;
        }
        String net_type = header.getString("net_type");
        if (StringUtils.isEmpty(net_type)) {
            return;
        }

        String user_id = "";
        if (os_name.equals("android")) user_id = header.getString("android_id");
        else user_id = device_id;

        //将header添加user_id
        header.put("user_id",user_id);
        //替换原来JSON的header为header
        json.put("header",header);


        //将结果输出
        k.set(json.toString());
        if (os_name.equals("android"))
            mo.write(k,NullWritable.get(),"android/");
        else
            mo.write(k,NullWritable.get(),"ios/");



    }

    //关闭资源
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mo.close();
        super.cleanup(context);
    }
}
