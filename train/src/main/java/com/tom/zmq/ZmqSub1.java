package com.tom.zmq;

import org.zeromq.ZMQ;

/**
 * ClassName: ZmqSub
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/9 17:10
 */
public class ZmqSub1 {
    public static void main(String[] args) {
        ZMQ.Context zmq = ZMQ.context(1);

        ZMQ.Socket sub = zmq.socket(ZMQ.SUB);
        sub.connect("tcp://localhost:6666");
        sub.connect("tcp://localhost:6667");
        sub.subscribe("".getBytes());
        while (!Thread.currentThread().isInterrupted()){
            byte[] recv = sub.recv();
            System.out.println(new String(recv));

        }
        sub.close();
        zmq.close();
    }
}
