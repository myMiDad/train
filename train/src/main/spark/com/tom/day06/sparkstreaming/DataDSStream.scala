package com.tom.day06.sparkstreaming

import com.tom.config.ConfigHelper
import com.tom.day06.sparkstreaming.utils.OffSetUtil
import com.tom.day06.utils.GetRedis
import org.apache.commons.lang.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * ClassName: DataDSStream
 * Description:
 *
 * @date 2019/12/16 16:05
 * @author Mi_dad
 */
object DataDSStream {
  private def save2Redis(result: RDD[(String, List[Int])]) = {
    result.reduceByKey((list1, list2) => list1.zip(list2).map(tp => tp._1 + tp._2))
      .foreachPartition(partition => {
        val jedis = GetRedis.getRedisConn(14)
        partition.foreach(tp => {
          jedis.hincrBy("trainDataOnLine", tp._1 + "_atpAll", tp._2(0))
          jedis.hincrBy("trainDataOnLine", tp._1 + "_main", tp._2(1))
          jedis.hincrBy("trainDataOnLine", tp._1 + "_wifi", tp._2(2))
          jedis.hincrBy("trainDataOnLine", tp._1 + "_balise", tp._2(3))
          jedis.hincrBy("trainDataOnLine", tp._1 + "_TCR", tp._2(4))
          jedis.hincrBy("trainDataOnLine", tp._1 + "_speed", tp._2(5))
          jedis.hincrBy("trainDataOnLine", tp._1 + "_DMI", tp._2(6))
          jedis.hincrBy("trainDataOnLine", tp._1 + "_TIU", tp._2(7))
          jedis.hincrBy("trainDataOnLine", tp._1 + "_JRU", tp._2(8))
        })
        jedis.close
      })
    }

    def main(args: Array[String]): Unit = {

      val conf = new SparkConf()
        .setAppName(this.getClass.getName)
        .setMaster("local[*]")
        //设置优雅的关闭
        .set("spark.streaming.stopGracefullyOnShutdown", "true")
        //接kafka的数据，基于网络编程的，序列化
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

      val sc = new SparkContext(conf)

      //创建一个接收器
      val ssc = new StreamingContext(sc, Seconds(1))

      //接收kafka的数据
      val kfStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
        //位置策略,如果说spark程序和kafka服务不在一个节点上，建议使用PreferConsistent
        //如果spark程序和kafka服务在一个节点上，建议使用PreferBrokers
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](
          //从配置文件中读取主题
          ConfigHelper.topics,
          //获取kafkaParams
          ConfigHelper.kafkaParams,
          //从数据库中读取偏移量信息
          OffSetUtil.readOffSet()
        )
      )
      //获取value的值
      kfStream.foreachRDD(rdd => {
        //首先判断RDD是否为空
        if (!rdd.isEmpty()) {
          //获取偏移量的数据
          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          println(offsetRanges.toBuffer+"============1")
          //获取value的值
          val value: RDD[String] = rdd.map(_.value())
          //处理数据
          val filtered: RDD[Array[String]] = value.map(_.split("\\|", -1)).filter(_.length >= 55)
          val result: RDD[(String, String, String, List[Int])] = filtered.map(arr => {
            val atpError: String = arr(17)
            val list: List[Int] = if (StringUtils.isNotEmpty(atpError)) {
              atpError match {
                case "车载主机" => List[Int](1, 1, 0, 0, 0, 0, 0, 0, 0)
                case "无线传输单元" => List[Int](1, 0, 1, 0, 0, 0, 0, 0, 0)
                case "应答器信息接收单元" => List[Int](1, 0, 0, 1, 0, 0, 0, 0, 0)
                case "轨道电路信息读取器" => List[Int](1, 0, 0, 0, 1, 0, 0, 0, 0)
                case "测色测距单元" => List[Int](1, 0, 0, 0, 0, 1, 0, 0, 0)
                case "人机交互接口单元" => List[Int](1, 0, 0, 0, 0, 0, 1, 0, 0)
                case "列车接口单元" => List[Int](1, 0, 0, 0, 0, 0, 0, 1, 0)
                case "司法记录单元" => List[Int](1, 0, 0, 0, 0, 0, 0, 0, 1)
                case _ => List[Int](1, 0, 0, 0, 0, 0, 0, 0, 0)
              }
            } else {
              List[Int](0, 0, 0, 0, 0, 0, 0, 0, 0)
            }

            //获取数据的时间
            val dateTime: String = arr(7)
            //截取小时时间
            val hour: String = dateTime.substring(0, 10)
            //截取分钟时间
            val minute: String = dateTime.substring(0, 12)
            //获取铁路局
            val railways_Bureau: String = arr(3)
            (hour, minute, railways_Bureau, list)
          })

          //将小时数据存储到redis中
          save2Redis(result.map(tp => (tp._1, tp._4)))
          //将分钟数据存储到redis中
          save2Redis(result.map(tp => (tp._2, tp._4)))
          //将铁路局数据存储到redis中
          save2Redis(result.map(tp => (tp._3, tp._4)))

          println(offsetRanges.toBuffer+"============2")
          //将偏移量存到mysql中
          OffSetUtil.saveOffSet(offsetRanges)
        }
      })
//      启动线程
      ssc.start()
      ssc.awaitTermination()
    }

  }
