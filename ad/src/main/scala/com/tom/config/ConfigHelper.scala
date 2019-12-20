package com.tom.config

import com.typesafe.config.{Config, ConfigFactory}

/**
 * ClassName: ConfigHelper
 * Description: 
 *
 * @date 2019/12/18 21:57
 * @author Mi_dad
 */
object ConfigHelper {
  //加载配置文件
  private lazy val load: Config = ConfigFactory.load()

  //加载序列化
  val serializer: String = load.getString("spark.serializer")
  //加载压缩
  val codec: String = load.getString("spark.codec")


}
