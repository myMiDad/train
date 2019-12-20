package com.tom.config

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * ClassName: ConfigHelper
 * Description:
 *
 * @date 2019/12/12 21:48
 * @author Mi_dad
 */
object ConfigHelper {
  //加载配置文件
  private lazy val load: Config = ConfigFactory.load()
  //加载序列化
  val serializer: String = load.getString("spark.serializer")
  //记载压缩
  val codec: String = load.getString("spark.codec")
  //加载数据库
  val driver: String = load.getString("db.default.driver")
  val url: String = load.getString("db.default.url")
  val user: String = load.getString("db.default.user")
  val password: String = load.getString("db.default.password")

  //加载topic
  val topics: Array[String]= load.getString("topic").split(",")
  //加载groupid
  val groupid: String = load.getString("groupid")
  //kafkaparams
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "hadoop201:9092,hadoop202:9092,hadoop203:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

}

