package com.tom.day06

import com.tom.config.ConfigHelper
import com.tom.day06.utils.GetRedis
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * ClassName: Data2Redis
 * Description: 
 *
 * @date 2019/12/16 13:45
 * @author Mi_dad
 */
object Data2Redis {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .config("spark.serializer",ConfigHelper.serializer)
      .getOrCreate()

    import session.implicits._

    //读取数据(出厂日期数据)
    val dateSource: Dataset[String] = session.read.textFile("E:\\test\\train\\data\\列车出厂时间数据.txt")

    val date: Dataset[(String, String)] = dateSource.map(line => {
      val splits: Array[String] = line.split("\\|")
      (splits(0), splits(1))
    })
    val trainDate: DataFrame = date.toDF("trainID","trainTime")

    //将出厂日期表广播到redis
    trainDate.filter(_.length>=2).foreachPartition(partition=>{
      val jedis = GetRedis.getRedisConn(15)
      partition.foreach(row=>{
        val trainID: String = row.getAs[String]("trainID")
        val trainTime: String = row.getAs[String]("trainTime")
        jedis.hset("trainDate",trainID,trainTime)
      })
    })

    session.stop()
  }

}
