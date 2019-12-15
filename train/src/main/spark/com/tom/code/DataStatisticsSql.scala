package com.tom.code

import java.util.Properties

import com.tom.config.ConfigHelper
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scalikejdbc.{DB, SQL, SQLUpdate}
import scalikejdbc.config.DBs

/**
 * ClassName: DataStatisticsSql
 * Description: 
 *
 * @date 2019/12/13 22:49
 * @author Mi_dad
 */
object DataStatisticsSql {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder().master("local[*]")
      .appName(this.getClass.getName)
      .getOrCreate()

    //读取数据
    val frame: DataFrame = session.read.parquet("E:\\test\\train\\data\\parquetData")
    //注册表
    frame.createTempView("logs")

    val sqlFrame: DataFrame = session.sql(
      """
        |select
        |MPacketHead_AttachRWBureau,
        |count(*) as sumData,
        |sum(if(MATPBaseInfo_AtpError != '',1,0)) as errorAll,
        |sum(case when MATPBaseInfo_AtpError = '车载主机' then 1 else 0 end) as main,
        |sum(if(MATPBaseInfo_AtpError = '无线传输单元',1,0)) as WIFI,
        |sum(if(MATPBaseInfo_AtpError = '应答器信息接收单元',1,0)) as balise,
        |sum(if(MATPBaseInfo_AtpError = '轨道电路信息读取器',1,0)) as TCR,
        |sum(if(MATPBaseInfo_AtpError = '测速测距单元',1,0)) as speed,
        |sum(if(MATPBaseInfo_AtpError = '人机交互接口单元',1,0)) as DMI,
        |sum(if(MATPBaseInfo_AtpError = '列车接口单元',1,0)) as TIU,
        |sum(if(MATPBaseInfo_AtpError = '司法记录单元',1,0)) as JUR
        |from logs
        |group by MPacketHead_AttachRWBureau
        |""".stripMargin)

    //使用dataframe
    //    val props = new Properties()
    //    props.setProperty("driver",ConfigHelper.driver)
    //    props.setProperty("user",ConfigHelper.user)
    //    props.setProperty("password",ConfigHelper.password)
    //
    //    sqlFrame.write.mode(SaveMode.Overwrite)
    //      .jdbc(ConfigHelper.url,"ATPData12132",props)

    //使用scalikejdbc
    DBs.setup()
    sqlFrame.rdd.foreachPartition(partition => {
      partition.foreach(tp => {
        val list = tp.toSeq
        DB.localTx { implicit session =>
          SQL("INSERT INTO atpdata1213 VALUES(?,?,?,?,?,?,?,?,?,?,?)")
            .bind(list(0), list(1), list(2), list(3), list(4), list(5), list(6), list(7), list(8), list(9), list(10))
            .update()
            .apply()
        }
      })
    })



    //释放资源
    session.stop()


  }

}
