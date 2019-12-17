package com.tom.day06

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * ClassName: TwoTableSelect
 * Description:
 *
 * @date 2019/12/16 8:59
 * @author Mi_dad
 */
object TwoTableSelectSql {
  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    import session.implicits._
    //读取高铁数据
    val frameInfo: DataFrame = session.read.parquet("E:\\test\\train\\data\\parquetData1")
    //注册高铁信息表
    frameInfo.createTempView("trainInfo")

    //列车出厂时间数据.txt
    //读取出厂日期表
    val timeDataSet: Dataset[(String, String)] = session.read.textFile("E:\\test\\train\\data\\列车出厂时间数据.txt").map(line => {
      val splits: Array[String] = line.split("\\|")
      (splits(0), splits(1))
    })
    //创建列车出厂日期表
    timeDataSet.toDF("TrainID", "TrainTime").createTempView("trainDate")
//    timeDataSet.toDF("TrainID", "TrainTime").show()
    //两表联查
    session.sql(
      """
        |select traintime,
        |sum(i.sumData),sum(i.errorAll),sum(i.main),sum(i.WIFI),
        |sum(i.balise),sum(i.TCR),sum(i.speed),sum(i.DMI),sum(i.TIU),sum(i.JUR)
        |from trainDate
        |join
        |(select
        |MPacketHead_TrainID,
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
        |from trainInfo
        |group by MPacketHead_TrainID) i
        |on i.MPacketHead_TrainID = TrainID
        |group by TrainTime
        |""".stripMargin).show()


    session.stop()



  }

}
