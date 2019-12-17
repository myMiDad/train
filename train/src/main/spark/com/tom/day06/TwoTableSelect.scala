package com.tom.day06

import com.tom.config.ConfigHelper
import com.tom.day06.utils.GetRedis
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import redis.clients.jedis.Jedis

/**
 * ClassName: TwoTableSelect
 * Description: 
 *
 * @date 2019/12/16 10:11
 * @author Mi_dad
 */
object TwoTableSelect {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .config("spark.serializer",ConfigHelper.serializer)
      .getOrCreate()

    import session.implicits._

    //读取数据(高铁详细数据)
    val trainInfo: DataFrame = session.read.load("E:\\test\\train\\data\\parquetData\\")

    val resultSource: Dataset[(String, List[Int])] = trainInfo.mapPartitions(partition => {
      val jedis: Jedis = GetRedis.getRedisConn(15)
      val tuples: Iterator[(String, List[Int])] = partition.map(row => {
        val atpError: String = row.getAs[String]("MATPBaseInfo_AtpError")
        val trainID: String = row.getAs[String]("MPacketHead_TrainID")
        //从redis获取出厂日期
        val trainTime = jedis.hget("trainDate", trainID)
        row.getAs[String]("MATPBaseInfo_AtpError") match {
          case "车载主机" => (trainTime, List[Int](1, 1, 1, 0, 0, 0, 0, 0, 0, 0))
          case "无线传输单元" => (trainTime, List[Int](1, 1, 0, 1, 0, 0, 0, 0, 0, 0))
          case "应答器信息接收单元" => (trainTime, List[Int](1, 1, 0, 0, 1, 0, 0, 0, 0, 0))
          case "轨道电路信息读取器" => (trainTime, List[Int](1, 1, 0, 0, 0, 1, 0, 0, 0, 0))
          case "测速测距单元" => (trainTime, List[Int](1, 1, 0, 0, 0, 0, 1, 0, 0, 0))
          case "人机交互接口单元" => (trainTime, List[Int](1, 1, 0, 0, 0, 0, 0, 1, 0, 0))
          case "列车接口单元" => (trainTime, List[Int](1, 1, 0, 0, 0, 0, 0, 0, 1, 0))
          case "司法记录单元" => (trainTime, List[Int](1, 1, 0, 0, 0, 0, 0, 0, 0, 1))
          case _ => (trainTime, List[Int](1, 0, 0, 0, 0, 0, 0, 0, 0, 0))
        }
      })
      jedis.close()
      tuples
    })

    val resultRDD: RDD[(String, List[Int])] = resultSource.rdd.reduceByKey((x, y) => x.zip(y).map(t => t._1 + t._2))
    resultRDD.foreach(println(_))

    session.stop()
  }

}
