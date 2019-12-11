package com.tom.zmq;

import org.zeromq.ZMQ;

/**
 * ClassName: ZmqResp
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/9 16:59
 */
public class ZmqResp {
    public static void main(String[] args) throws Exception{
        //创建zmq实例
        ZMQ.Context zmq = ZMQ.context(1);
        //创建resp实例
        ZMQ.Socket resp = zmq.socket(ZMQ.REP);
        resp.bind("tcp://localhost:6666");
        int i=0;
        while (true){
            byte[] recv = resp.recv();
            System.out.println(new String(recv));

            resp.send("world"+"-"+i++);
            Thread.sleep(500);


        }

    }
}
