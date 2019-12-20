package com.tom.statistics

import com.tom.config.ConfigHelper
import org.apache.spark.sql.SparkSession

/**
 * ClassName: DataStatistics
 * Description: 
 *
 * @date 2019/12/18 23:17
 * @author Mi_dad
 */
object DataStatistics {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .config("spark.serializer",ConfigHelper.serializer)
      .config("spark.code",ConfigHelper.codec)
      .getOrCreate()

    import session.implicits._

    val frame = session.read.parquet("E:\\test\\ad\\parquent")

    frame.createTempView("data")
    session.sql(
      """
        |select a.provincename,a.cityname,b.ct
        |from
        | (select distinct provincename,cityname from data) a,
        | (select count(*) ct,cityname
        |   from
        |     (select provincename ,cityname from data) d
        |   group by cityname
        |   ) b
        |where a.cityname = b.cityname
        |order by a.provincename
        |""".stripMargin).show(10000)

    println("================================================================")
//    session.sql(
//      """
//        | select count(*) ct,cityname
//        |   from
//        |     (select provincename ,cityname from data) d
//        |   group by cityname
//        |""".stripMargin).show(100000)


    session.stop()
  }

}
