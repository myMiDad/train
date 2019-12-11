package com.tom.datasource;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import dms.MATPOuterClass;
import dms.MTransfCtrlOuterClass;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.zeromq.ZMQ;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * ClassName: Zmq2Kafka
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/10 9:42
 */
public class Zmq2Kafka {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //zmq
        ZMQ.Context zmq = ZMQ.context(1);
        //创建一个订阅模式
        ZMQ.Socket sub = zmq.socket(ZMQ.SUB);
        //连接ip和端口
        sub.connect("tcp://localhost:6666");
        //订阅模式
        sub.subscribe("".getBytes());

        Properties prop = new Properties();
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092");

        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        while (true) {
            byte[] recv = sub.recv();

            MTransfCtrlOuterClass.MTransfCtrl mTransfCtrl = MTransfCtrlOuterClass.MTransfCtrl.parseFrom(recv);
            //先判断是否存在MsgType字段，并且值为1
            if (mTransfCtrl.hasMsgtype() && mTransfCtrl.getMsgtype() == 1 && mTransfCtrl.hasData()) {
                //在字段存在情况下判断数据data是否存在
                //获取data数据
                ByteString data = mTransfCtrl.getData();
                //将data数据转化
                MATPOuterClass.MATP matp = MATPOuterClass.MATP.parseFrom(data);
                //获取MPacketHead 头数据包
                int atpType = 0;
                int trainID = 0;
                ByteString trainNum = null;
                int attachRWBureau = 0;
                int viaRWBureau = 0;
                boolean crossDayTrainNum = false;
                ByteString driverID = null;
                if (matp.hasPacketHead()) {
                    MATPOuterClass.MPacketHead packetHead = matp.getPacketHead();
                    //获取atpType
                    if (packetHead.hasATPType()) {
                        atpType = packetHead.getATPType();
                    }
                    if (packetHead.hasTrainID()) {
                        trainID = packetHead.getTrainID();
                    }
                    if (packetHead.hasTrainNum()) {
                        trainNum = packetHead.getTrainNum();
                    }
                    if (packetHead.hasAttachRWBureau()) {
                        attachRWBureau = packetHead.getAttachRWBureau();
                    }
                    if (packetHead.hasViaRWBureau()) {
                        viaRWBureau = packetHead.getViaRWBureau();
                    }
                    if (packetHead.hasCrossDayTrainNum()) {
                        crossDayTrainNum = packetHead.getCrossDayTrainNum();
                    }
                    if (packetHead.hasDriverID()) {
                        driverID = packetHead.getDriverID();
                    }
                }
                //获取MATPBaseInfo ATP基本信息包（包括车载主机、无线传输单元、应答器信息接收单元、轨道电路信息读取器（TCR）、测速测距单元、人机交互接口单元（DMI）、列车接口单元（TIU）、司法记录单元（JRU）等多个子系统）
                int dataTime;
                int speed;
                int level;
                int mileage;
                int braking;
                int emergentBrakSpd;
                int commonBrakSpd;
                int runDistance;
                int direction;
                int lineID;
                ByteString atpError;
                if (matp.hasATPBaseInfo()) {
                    MATPOuterClass.MATPBaseInfo atpBaseInfo = matp.getATPBaseInfo();
                    if (atpBaseInfo.hasDataTime()) {
                        dataTime = atpBaseInfo.getDataTime();
                    }
                    if (atpBaseInfo.hasSpeed()) {
                        speed = atpBaseInfo.getSpeed();
                    }
                    if (atpBaseInfo.hasLevel()) {
                        level = atpBaseInfo.getLevel();
                    }
                    if (atpBaseInfo.hasMileage()) {
                        mileage = atpBaseInfo.getMileage();
                    }
                    if (atpBaseInfo.hasBraking()) {
                        braking = atpBaseInfo.getBraking();
                    }
                    if (atpBaseInfo.hasEmergentBrakSpd()) {
                        emergentBrakSpd = atpBaseInfo.getEmergentBrakSpd();
                    }
                    if (atpBaseInfo.hasCommonBrakSpd()) {
                        commonBrakSpd = atpBaseInfo.getCommonBrakSpd();
                    }
                    if (atpBaseInfo.hasRunDistance()) {
                        runDistance = atpBaseInfo.getRunDistance();
                    }
                    if (atpBaseInfo.hasDirection()) {
                        direction = atpBaseInfo.getDirection();
                    }
                    if (atpBaseInfo.hasLineID()) {
                        lineID = atpBaseInfo.getLineID();
                    }
                    if (atpBaseInfo.hasATPError()) {
                        atpError = atpBaseInfo.getATPError();
                    }
                }
                //获取MBalisePocket 应答器数据包（道岔、转辙机、轨道电路、应答器）
                if (matp.hasBalisePocket()) {
                    MATPOuterClass.MBalisePocket balisePocket = matp.getBalisePocket();
                    if (balisePocket.hasBaliseID()) {
                        int baliseID = balisePocket.getBaliseID();
                    }
                    if (balisePocket.hasBaliseMile()) {
                        int baliseMile = balisePocket.getBaliseMile();
                    }
                    if (balisePocket.hasBaliseType()) {
                        int baliseType = balisePocket.getBaliseType();
                    }
                    if (balisePocket.hasDirection()) {
                        int direction1 = balisePocket.getDirection();
                    }
                    if (balisePocket.hasLineID()) {
                        int lineID1 = balisePocket.getLineID();
                    }
                    if (balisePocket.hasAttachRWBureau()) {
                        int attachRWBureau1 = balisePocket.getAttachRWBureau();
                    }
                    if (balisePocket.hasBaliseNum()) {
                        ByteString baliseNum = balisePocket.getBaliseNum();
                    }
                    if (balisePocket.hasStation()) {
                        ByteString station = balisePocket.getStation();
                    }
                    if (balisePocket.hasBaliseError()) {
                        ByteString baliseError = balisePocket.getBaliseError();
                    }
                }
                //获取Signal 当前信号机信息包（电源、灯泡、开灯继电器、信号机接口电路）
                int longitude=0;
                int latitude=0;
                if (matp.hasSignal()) {
                    MATPOuterClass.MSignal signal = matp.getSignal();
                    if (signal.hasSignalID()) {
                        int signalID = signal.getSignalID();
                    }
                    if (signal.hasSignalName()) {
                        ByteString signalName = signal.getSignalName();
                    }
                    if (signal.hasStation()) {
                        ByteString station = signal.getStation();
                    }
                    if (signal.hasSignalMile()) {
                        int signalMile = signal.getSignalMile();
                    }
                    if (signal.hasDirection()) {
                        int direction1 = signal.getDirection();
                    }
                    if (signal.hasLineID()) {
                        int lineID1 = signal.getLineID();
                    }
                    if (signal.hasLongitude()) {
                        longitude = signal.getLongitude();
                    }
                    if (signal.hasLatitude()) {
                        latitude = signal.getLatitude();
                    }
                    if (signal.hasSignalError()) {
                        ByteString signalError = signal.getSignalError();
                    }
                }
                //获取
                //获取

                String weatherInfo = WeatherJson.getWeatherInfo(longitude, latitude);
                StringBuffer sb = new StringBuffer();
                sb.append(atpType).append("|").append(trainID).append("|").append(trainNum).append("|")
                        .append(attachRWBureau).append("|").append(viaRWBureau).append("|")
                        .append(crossDayTrainNum).append("|").append(driverID).append("|")
                        .append(weatherInfo);

                //将数据发送给kafka
                producer.send(new ProducerRecord<>("projectSource", sb.toString()));
            }
        }


    }
}
