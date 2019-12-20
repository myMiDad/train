package com.tom.tags

import com.tom.config.ConfigHelper
import com.tom.utils.{GetRedis, TurnType}
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * ClassName: AdTag
 * Description: 
 *
 * @date 2019/12/19 10:01
 * @author Mi_dad
 */
object AdTag {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .config("spark.codec", ConfigHelper.codec)
      .config("spark.serializer", ConfigHelper.serializer)
      .getOrCreate()

    import session.implicits._
    //读取数据源
    val source: DataFrame = session.read.parquet("E:\\test\\ad\\parquet")
    //读取app字典
    val appSource: Dataset[String] = session.read.textFile("E:\\test\\ad\\data\\app_dict.txt")
    //将字典存入redis
    appSource.map(_.split("\t", -1)).filter(_.length >= 4).foreachPartition(partition => {
      val jedis = GetRedis.getRedisConn(2)
      partition.foreach(arr => {
        jedis.hset("appDict", arr(4), arr(1))
      })
      jedis.close()
    })

    //读取stopWords文件
    val stopWordsSource: Dataset[String] = session.read.textFile("E:\\test\\ad\\data\\stopwords.txt")
    //将stopwords广播
    val keyWords: Map[String, Int] = stopWordsSource.coalesce(1).map((_, 1)).collect().toMap
    val keyWordsBro: Broadcast[Map[String, Int]] = session.sparkContext.broadcast(keyWords)
    val ds: Dataset[(String, List[(String, Long)])] = source.map(row => {

      //首先获取用户唯一标识
      val imei = row.getAs[String]("imei")
      val mac = row.getAs[String]("mac")
      val idfa = row.getAs[String]("idfa")
      val openudid = row.getAs[String]("openudid")
      val androidid = row.getAs[String]("androidid")
      val imeimd5 = row.getAs[String]("imeimd5")
      val macmd5 = row.getAs[String]("macmd5")
      val idfamd5 = row.getAs[String]("idfamd5")
      val openudidmd5 = row.getAs[String]("openudidmd5")
      val androididmd5 = row.getAs[String]("androididmd5")
      val imeisha1 = row.getAs[String]("imeisha1")
      val macsha1 = row.getAs[String]("macsha1")
      val idfasha1 = row.getAs[String]("idfasha1")
      val openudidsha1 = row.getAs[String]("openudidsha1")
      val androididsha1 = row.getAs[String]("androididsha1")


      var userID = ""
      if (!StringUtils.isEmpty(imei)) {
        userID = imei
      } else if (!StringUtils.isEmpty(mac)) {
        userID = mac
      } else if (!StringUtils.isEmpty(idfa)) {
        userID = idfa
      } else if (!StringUtils.isEmpty(openudid)) {
        userID = openudid
      } else if (!StringUtils.isEmpty(androidid)) {
        userID = androidid
      } else if (!StringUtils.isEmpty(imeimd5)) {
        userID = imeimd5
      } else if (!StringUtils.isEmpty(macmd5)) {
        userID = macmd5
      } else if (!StringUtils.isEmpty(idfamd5)) {
        userID = idfamd5
      } else if (!StringUtils.isEmpty(openudidmd5)) {
        userID = openudidmd5
      } else if (!StringUtils.isEmpty(androididmd5)) {
        userID = androididmd5
      } else if (!StringUtils.isEmpty(imeisha1)) {
        userID = imeisha1
      } else if (!StringUtils.isEmpty(macsha1)) {
        userID = macsha1
      } else if (!StringUtils.isEmpty(idfasha1)) {
        userID = idfasha1
      } else if (!StringUtils.isEmpty(openudidsha1)) {
        userID = openudidsha1
      } else if (!StringUtils.isEmpty(androididsha1)) {
        userID = androididsha1
      } else {
        null
      }
      //创建一个list存放标签
      var list: List[(String, Long)] = List()
      //获取广告位类型
      val adspacetype: Integer = row.getAs[Integer]("adspacetype")
      if (adspacetype.toInt < 10) {
        list :+= ("LC0" + adspacetype, 1L)
      } else if (adspacetype >= 10) {
        list :+= ("LC" + adspacetype, 1L)
      }
      //获取广告位名称
      val adspacetypename: String = row.getAs[String]("adspacetypename")
      if (!StringUtils.isEmpty(adspacetypename))
        list :+= ("LN " + adspacetypename, 1L)

      //获取App名称
      //先获取Appname
      val appname = row.getAs[String]("appname")
      if (!StringUtils.isEmpty(appname)) {
        list :+= ("APP" + appname, 1L)
      } else {
        val appid = row.getAs[String]("appid")
        if (StringUtils.isNotEmpty(appid)) {
          val jedis = GetRedis.getRedisConn(2)
          val value = jedis.hget("appDict", appid)
          if (StringUtils.isNotEmpty(value))
            list :+= ("APP" + value, 1L)
        }
      }
      //渠道
      val adplatformproviderid: Integer = row.getAs[Integer]("adplatformproviderid")
      if (TurnType.integer2Int(adplatformproviderid) != 0) {
        list :+= ("CN" + adplatformproviderid, 1L)
      }
      //设备
      //操作系统
      val client = row.getAs[Integer]("client")
      val clientInt = TurnType.integer2Int(client)
      if (clientInt != 0) {
        if (clientInt == 1)
          list :+= ("Android", 1L)
        else if (clientInt == 2)
          list :+= ("IOS", 1L)
        else if (clientInt.toInt == 3)
          list :+= ("WP", 1L)
        else
          list :+= ("_", 1L)
      }
      //运营商名称
      val ispname = row.getAs[String]("ispname")
      ispname match {
        case "移动" => list :+= ("D00030001", 1L)
        case "联通" => list :+= ("D00030002", 1L)
        case "电信" => list :+= ("D00030003", 1L)
        case _ => list :+= ("D00030004", 1L)
      }

      //联网方式
      val networkmannername = row.getAs[String]("networkmannername")
      if (StringUtils.isNotEmpty(networkmannername))
        list :+= (networkmannername, 1L)

      //keywords
      val keywords: String = row.getAs[String]("keywords")
      if (StringUtils.isNotEmpty(keywords)) {
        val splits: Array[String] = keywords.split("\\|", -1).filter(key => key.length >= 3 && key.length <= 8 && !keyWordsBro.value.contains(key))
        splits.foreach(str => {
          list :+= (str, 1L)
        })
      }
      //地域标签
      val provincename = row.getAs[String]("provincename")
      if (StringUtils.isNotEmpty(provincename))
        list :+= ("ZP" + provincename, 1L)
      val cityname = row.getAs[String]("cityname")
      if (StringUtils.isNotEmpty(cityname))
        list :+= ("ZC" + cityname, 1L)

      //商圈标签

      (userID, list.groupBy(_._1).mapValues(tp => tp.map(_._2).sum).toList)
    }
    )
    //ds.show(10000)
    val value: RDD[(String, List[(String, Long)])] = ds.rdd.reduceByKey(
      (list1, list2) => (list1 ++ list2).groupBy(_._1).mapValues(tp => tp.map(_._2).sum).toList
    )
    value.foreach(tp => println(tp._1 + "=======" + tp._2))



    session.stop()
  }
}
