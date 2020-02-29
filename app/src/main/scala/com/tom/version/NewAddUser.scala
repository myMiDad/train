package com.tom.version

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * ClassName: NewAddUser
 * Description:
 *
 * @date 2019/12/23 16:23
 * @author Mi_dad
 */
object NewAddUser {
  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .getOrCreate()

    import session.implicits._

    val source: RDD[String] = session.sparkContext.textFile("D:\\z_test\\app\\AddUser2TableDriver2\\part-r-00000")

    source.map(_.split(",", -1)).filter(_.length > 5)


    //关闭资源
    session.close()
  }
}

case class User_Info(
                      day: String,
                      app_token: String,
                      user_id: String,
                      version: String,
                      channel: String,
                      city: String
                    )

case class User_old_Info(
                          day: String,
                          app_token: String,
                          user_id: String,
                          version: String
                        )