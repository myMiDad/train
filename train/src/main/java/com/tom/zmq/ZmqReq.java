package com.tom.zmq;

import org.zeromq.ZMQ;

import java.util.Arrays;

/**
 * ClassName: ZmqReq
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/9 16:53
 */
public class ZmqReq {
    public static void main(String[] args) {
        //创建一个zmq实例
        ZMQ.Context zmq = ZMQ.context(1);
        //创建一个问的实例
        ZMQ.Socket req = zmq.socket(ZMQ.REQ);
        req.connect("tcp://localhost:6666");
        int i = 0;
        //与resp进行交互
        while(!Thread.currentThread().isInterrupted()){
            //向response发送消息
            String line = "hello "+i;
            System.out.println(line);
            req.send(line);
            i += 1;
            //接收resp的消息
            byte[] recv = req.recv();
            System.out.println(new String(recv));
        }

        req.close();
        zmq.close();


    }
}
