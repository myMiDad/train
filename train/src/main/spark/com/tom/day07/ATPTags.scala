package com.tom.day07

import java.text.SimpleDateFormat
import java.util.Date

import com.tom.config.ConfigHelper
import com.tom.day07.utils.WeatherUtil
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * ClassName: ATPTags
 * Description:
 *
 * @date 2019/12/17 15:44
 * @author Mi_dad
 */
object ATPTags {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .config("spark.serializer", ConfigHelper.serializer)
      .getOrCreate()

    //导入隐式转换
    import session.implicits._

    //读取parquet文件数据
    val frame: DataFrame = session.read.parquet("E:\\test\\train\\data\\parquetData1\\").cache()

    //读取出厂时间数据并进行简单处理
    val trainTimeMap: collection.Map[String, String] = session.sparkContext.textFile("E:\\test\\train\\data\\列车出厂时间数据.txt")
      .map(_.split("\\|", -1)).filter(_.length >= 2).map(arr => (arr(0), arr(1))).collectAsMap()
    val trainTimeBro: Broadcast[collection.Map[String, String]] = session.sparkContext.broadcast(trainTimeMap)

    //读取ATP检修台账数据并进行简单处理
    val trainCheckMap: collection.Map[String, (String, String, String)] = session.sparkContext.textFile("E:\\test\\train\\data\\ATP检修台账.txt")
      .map(_.split("\\|", -1)).filter(_.length >= 4).map(arr => (arr(0), (arr(1), arr(2), arr(3)))).collectAsMap()
    val trainCheckBro: Broadcast[collection.Map[String, (String, String, String)]] = session.sparkContext.broadcast(trainCheckMap)

    //读取生产厂家数据并进行简单处理
    val trainFaMap: collection.Map[String, String] = session.sparkContext.textFile("E:\\test\\train\\data\\列车生产厂家.txt")
      .map(_.split("\\|", -1)).filter(_.length >= 2).map(arr => (arr(0), arr(1))).collectAsMap()
    val trainFaBor: Broadcast[collection.Map[String, String]] = session.sparkContext.broadcast(trainFaMap)

    val countTags: RDD[(String, List[(String, Long)])] = frame.map(row => {
      //获取唯一标识MPacketHead_TrainID
      val trainID: String = row.getAs[String]("MPacketHead_TrainID")
      //获取ATPError
      val atpError: String = row.getAs[String]("MATPBaseInfo_AtpError")
      //创建一个容器来存放各种标签
      var list: List[(String, Long)] = List[(String, Long)]()
      //判断atpError是否为空
      if (!StringUtils.isEmpty(atpError)) {
        //获取温度、湿度、天气、速度等级
        val temLevel = WeatherUtil.getTemLevel(row)
        val humLevel = WeatherUtil.getHumLevel(row)
        val weatherLevel = WeatherUtil.getWeatherLevel(row)
        val speedLevel = WeatherUtil.getSpeedLevel(row)
        //打标签
        list :+= ("PA" + temLevel + "/" + humLevel + "/" + weatherLevel + "/" + speedLevel + ":" + atpError, 1L)
      }

      //出厂时间标签
      val trainTime: String = trainTimeBro.value.getOrElse(trainID, "无返回值")
      if (!trainTime.equals("无返回值")) {
        list :+= ("TI" + trainTime, 1L)
      }
      //ATP类型
      val atpType = row.getAs[String]("MPacketHead_ATPType")
      if (StringUtils.isEmpty(atpError)) {
        list :+= ("TY" + atpType, 1L)
      }
      //司机标签
      val driverID = row.getAs[String]("DriverInfo_DriverID")
      if (StringUtils.isNotEmpty(driverID)) {
        list :+= ("DR" + driverID, 1L)
      }
      //厂家标签
      val trainFa = trainFaBor.value.getOrElse(trainID.substring(0, 1), "无返回值")
      if (!trainFa.equals("无返回值")) {
        list :+= ("FA" + trainFa, 1L)
      }
      //检修更换标签
      val tuple = trainCheckBro.value.getOrElse(trainID, ("", "", ""))
      if (tuple._1.equals("检修")) {
        list :+= ("AJX" + tuple._2 + tuple._3, 1L)
      } else {
        list :+= ("AGH" + tuple._2 + tuple._3, 1L)
      }
      (trainID, list)
    }).rdd.reduceByKey({
      (list1, list2) => (list1 ++ list2).groupBy(_._1).mapValues(tp => tp.map(_._2).sum).toList
    })

    //时长标签
    val timeTags: RDD[(String, List[(String, Long)])] = frame.map(row => {
      //获取唯一标识
      val trainID: String = row.getAs[String]("MPacketHead_TrainID")
      //获取温度、湿度、天气、速度等级
      val temLevel = WeatherUtil.getTemLevel(row)
      val humLevel = WeatherUtil.getHumLevel(row)
      val weatherLevel = WeatherUtil.getWeatherLevel(row)
      val speedLevel = WeatherUtil.getSpeedLevel(row)
      //获取数据时间
      val dataTimeStr: String = row.getAs[String]("MATPBaseInfo_DataTime")
      val format: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
      val time: Long = format.parse(dataTimeStr).getTime
      //生成标签
      //创建一个容器用来存放各种标签
      var list: List[(String, Long)] = List[(String, Long)]()
      list :+= (temLevel, time)
      list :+= (humLevel, time)
      list :+= (weatherLevel, time)
      list :+= (speedLevel, time)
      (trainID, list)
    }).rdd.coalesce(1).sortBy(_._2(0)._2).reduceByKey((list1, list2) => list1 ++ list2)/*.foreach(tp=>println(tp._1+"===="+tp._2))*/
      .map(tp => {
        val list: List[(String, Long)] = tp._2.sliding(5).map(t => (t.head._1, t.last._2 - t.head._2))
          .toList
          .groupBy(_._1).mapValues(v => v.map(_._2).sum).toList
        (tp._1, list)
      })
//      countTags.foreach(tp=>println(tp._1+"===="+tp._2))
    timeTags.foreach(tp=>tp._2.foreach(l=>println(l._2/1000/3600/24)))
    val result: RDD[(String, List[(String, Long)])] = countTags.union(timeTags)

//    result.reduceByKey((list1,list2)=>list1 ++ list2).foreach(tp=>{
//      tp._2.foreach(t=>println(t._2))
//    })

    session.stop()
  }
}
