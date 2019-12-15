package com.tom.config

import com.typesafe.config.{Config, ConfigFactory}

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
  val codec: String = load.getString("spark.code")
  //加载数据库
  val driver: String = load.getString("db.default.driver")
  val url: String = load.getString("db.default.url")
  val user: String = load.getString("db.default.user")
  val password: String = load.getString("db.default.password")
}

