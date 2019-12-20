package com.tom.day06

import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis

/**
 * ClassName: Test
 * Description: 
 *
 * @date 2019/12/16 11:06
 * @author Mi_dad
 */
object Test {
  def main(args: Array[String]): Unit = {
//    val jedis = new Jedis("hadoop201",6379)
//    print(jedis.hget("trainDate","2009084"))

    val session = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    import session.implicits._

    val frame: DataFrame = session.read.load("E:\\test\\train\\data\\parquetData1\\")

    frame.show()
//    println(frame.count())

//    frame.createTempView("logs")
//    session.sql(
//      """
//        |select count(*) from logs
//        |""".stripMargin).show()

  }

}
