package com.tom.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: FlumeInterceptor
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/11 17:16
 */
public class FlumeInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String line = new String(body);
        String[] splits = line.split("," ,- 1);
        if (splits.length>=2){
            return event;
        }else {
            return null;
        }
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> list = new ArrayList<>();
        for (Event e:events){
            if (e!=null){
                list.add(e);
            }
        }
        return list;
    }

    @Override
    public void close() {

    }
    public static class builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new FlumeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
