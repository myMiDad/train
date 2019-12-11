package com.tom.datasource;

import jdk.nashorn.internal.parser.Scanner;
import org.zeromq.ZMQ;

/**
 * ClassName: ZmqPub
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/10 10:54
 */
public class ZmqPub {
    public static void main(String[] args) {
        ZMQ.Context zmq = ZMQ.context(1);
        ZMQ.Socket pub = zmq.socket(ZMQ.PUB);
        pub.bind("tcp://localhost:6666");



    }
}
