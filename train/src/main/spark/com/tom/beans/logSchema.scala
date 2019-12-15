package com.tom.beans

import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * ClassName: Schema
 * Description: 
 *
 * @date 2019/12/12 21:27
 * @author Mi_dad
 */
object logSchema {
  val schema = StructType(Seq(
    StructField("MPacketHead_ATPType",StringType),
    StructField("MPacketHead_TrainID",StringType),
    StructField("MPacketHead_TrainNum",StringType),
    StructField("MPacketHead_AttachRWBureau",StringType),
    StructField("MPacketHead_ViaRWBureau",StringType),
    StructField("MPacketHead_CrossDayTrainNum",StringType),
    StructField("MPacketHead_DriverIDMATPBaseInfo_DataTime",StringType),
    StructField("DataTime",StringType),
    StructField("MATPBaseInfo_Speed",StringType),
    StructField("MATPBaseInfo_Level",StringType),
    StructField("MATPBaseInfo_Mileage",StringType),
    StructField("MATPBaseInfo_Braking",StringType),
    StructField("MATPBaseInfo_EmergentBrakSpd",StringType),
    StructField("MATPBaseInfo_CommonBrakSpd",StringType),
    StructField("MATPBaseInfo_RunDistance",StringType),
    StructField("MATPBaseInfo_Direction",StringType),
    StructField("MATPBaseInfo_LineID",StringType),
    StructField("MATPBaseInfo_AtpError",StringType),
    StructField("MBalisePocket_BaliseID",StringType),
    StructField("MBalisePocket_BaliseMile",StringType),
    StructField("MBalisePocket_BaliseType",StringType),
    StructField("MBalisePocket_Direction",StringType),
    StructField("MBalisePocket_LineID",StringType),
    StructField("MBalisePocket_AttachRWBureau",StringType),
    StructField("MBalisePocket_BaliseNum",StringType),
    StructField("MBalisePocket_Station",StringType),
    StructField("MBalisePocket_BaliseError",StringType),
    StructField("Signal_SignalID",StringType),
    StructField("Signal_SignalName",StringType),
    StructField("Signal_Station",StringType),
    StructField("Signal_SignalMile",StringType),
    StructField("Signal_Direction",StringType),
    StructField("Signal_LineID",StringType),
    StructField("Signal_Longitude",StringType),
    StructField("Signal_Latitude",StringType),
    StructField("Signal_SignalError",StringType),
    StructField("RunNextSignal_SignalID",StringType),
    StructField("RunNextSignal_SignalName",StringType),
    StructField("RunNextSignal_Station",StringType),
    StructField("RunNextSignal_SignalMile",StringType),
    StructField("RunNextSignal_Direction",StringType),
    StructField("RunNextSignal_LineID",StringType),
    StructField("RunNextSignal_Longitude",StringType),
    StructField("RunNextSignal_Latitude",StringType),
    StructField("DriverInfo_DriverID",StringType),
    StructField("DriverInfo_DriverName",StringType),
    StructField("DriverInfo_DriverPhone",StringType),
    StructField("DriverInfo_DriverOption",StringType),
    StructField("DriverInfo_Validit",StringType),
    StructField("RunDirection_RunDirection",StringType),
    StructField("UUID",StringType),
    StructField("Environment_Temperature",StringType),
    StructField("Environment_Road",StringType),
    StructField("Environment_Weather",StringType),
    StructField("Environment_Humidity",StringType)
  ))
}
