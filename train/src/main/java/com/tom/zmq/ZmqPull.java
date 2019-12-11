package com.tom.zmq;

import org.zeromq.ZMQ;

/**
 * ClassName: ZmqPull
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/9 17:18
 */
public class ZmqPull {
    public static void main(String[] args) {
        ZMQ.Context zmq = ZMQ.context(1);
        ZMQ.Socket pull = zmq.socket(ZMQ.PULL);
        pull.connect("tcp://localhost:6666");
        while (!Thread.currentThread().isInterrupted()){
            byte[] recv = pull.recv();
            System.out.println(new String(recv));
        }
        pull.close();
        zmq.term();
    }
}
