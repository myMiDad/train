package com.tom.zmq;

import org.zeromq.ZMQ;

/**
 * ClassName: ZmqPub
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/9 17:07
 */
public class ZmqPub {
    public static void main(String[] args) throws Exception {
        ZMQ.Context zmq = ZMQ.context(1);
        ZMQ.Socket pub = zmq.socket(ZMQ.PUB);

        pub.bind("tcp://localhost:6666");

        int i=0;
        while (true){
            Thread.sleep(500);
            pub.send("第"+i+"条消息。。。");
            i+=1;
        }
    }
}
