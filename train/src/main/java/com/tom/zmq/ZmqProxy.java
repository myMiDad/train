package com.tom.zmq;

import org.zeromq.ZMQ;

/**
 * ClassName: ZmqProxy
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/9 18:58
 */
public class ZmqProxy {
    public static void main(String[] args) {
        ZMQ.Context zmq = ZMQ.context(1);

        ZMQ.Socket sub = zmq.socket(ZMQ.SUB);
        sub.connect("tcp://localhost:6666");
        sub.subscribe("".getBytes());

        ZMQ.Socket pub = zmq.socket(ZMQ.PUB);
        pub.bind("tcp://localhost:6668");
        ZMQ.proxy(sub, pub, null);

        pub.close();
        sub.close();
        zmq.close();
    }
}
