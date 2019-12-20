package com.tom.test

import com.tom.config.ConfigHelper
import org.apache.spark.sql.SparkSession

/**
 * ClassName: Test
 * Description: 
 *
 * @date 2019/12/18 23:01
 * @author Mi_dad
 */
object Test {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .config("spark.serializer",ConfigHelper.serializer)
      .config("spark.code",ConfigHelper.codec)
      .getOrCreate()

    import session.implicits._
    session.read.parquet("E:\\test\\ad\\parquent").show()


    session.stop()
  }

}
