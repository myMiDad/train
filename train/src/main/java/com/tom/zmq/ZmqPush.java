package com.tom.zmq;

import org.zeromq.ZMQ;

/**
 * ClassName: ZmqPush
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/9 17:16
 */
public class ZmqPush {
    public static void main(String[] args) throws Exception {
        ZMQ.Context zmq = ZMQ.context(1);
        ZMQ.Socket push = zmq.socket(ZMQ.PUSH);
        push.bind("tcp://localhost:6666");
        int i=0;
        while (true){
            push.send("消息"+i);
            i+=1;
            Thread.sleep(500);
        }
    }
}
