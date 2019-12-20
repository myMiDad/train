package com.tom.code

import java.io.File
import java.net.URI
import java.util.Properties

import com.google.gson.Gson
import com.tom.config.ConfigHelper
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * ClassName: DataStatistics
 * Description: 
 *
 * @date 2019/12/13 10:02
 * @author Mi_dad
 */
object DataStatistics {
  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    import session.implicits._

    val frame: DataFrame = session.read.load("E:\\test\\train\\data\\parquetData\\")
    //使用sparkCore实现
    //    frame.show()

    val resultSource: Dataset[(String, List[Int])] = frame.map(row => {
      val atpError: String = row.getAs[String]("MATPBaseInfo_AtpError")

      val MPacketHead_AttachRWBureau: String = row.getAs[String]("MPacketHead_AttachRWBureau")

      row.getAs[String]("MATPBaseInfo_AtpError") match {
        case "车载主机" => (MPacketHead_AttachRWBureau, List[Int](1, 1, 1, 0, 0, 0, 0, 0, 0, 0))
        case "无线传输单元" => (MPacketHead_AttachRWBureau, List[Int](1, 1, 0, 1, 0, 0, 0, 0, 0, 0))
        case "应答器信息接收单元" => (MPacketHead_AttachRWBureau, List[Int](1, 1, 0, 0, 1, 0, 0, 0, 0, 0))
        case "轨道电路信息读取器" => (MPacketHead_AttachRWBureau, List[Int](1, 1, 0, 0, 0, 1, 0, 0, 0, 0))
        case "测速测距单元" => (MPacketHead_AttachRWBureau, List[Int](1, 1, 0, 0, 0, 0, 1, 0, 0, 0))
        case "人机交互接口单元" => (MPacketHead_AttachRWBureau, List[Int](1, 1, 0, 0, 0, 0, 0, 1, 0, 0))
        case "列车接口单元" => (MPacketHead_AttachRWBureau, List[Int](1, 1, 0, 0, 0, 0, 0, 0, 1, 0))
        case "司法记录单元" => (MPacketHead_AttachRWBureau, List[Int](1, 1, 0, 0, 0, 0, 0, 0, 0, 1))
        case _ => (MPacketHead_AttachRWBureau, List[Int](1, 0, 0, 0, 0, 0, 0, 0, 0, 0))
      }
    })
    val resultRDD: RDD[(String, List[Int])] = resultSource.rdd.reduceByKey((x, y) => x.zip(y).map(t => t._1 + t._2))

    //    resultRDD.foreach(println)

    //    方式一：使用dataframe
    //    val result: DataFrame = resultSource.rdd.reduceByKey((x, y) => x.zip(y).map(t => t._1 + t._2)).map(tp => {
    //      (tp._1, tp._2(0), tp._2(1), tp._2(2), tp._2(3), tp._2(4), tp._2(5), tp._2(6), tp._2(7), tp._2(8), tp._2(9))
    //    }).toDF(
    //      "铁路局", "总数据量", "总报警次数", "车载主机", "无线传输单元", "应答器信息接收单元", "轨道电路信息读取器", "测速测距单元",
    //      "人机交互接口单元", "列车接口单元", "司法记录单元")
    //    result.write.mode(SaveMode.Overwrite).partitionBy("铁路局").json("E:\\test\\train\\data\\jsonOutputDataFrame\\")
    //    result.show()

    //方式二：使用Gson
    //向本地写
    //    val file: File = new File("E:\\test\\train\\data\\jsonOutPutGson\\")
    //判断文佳是否存在，如果存在删除原来已经存在的
    //        if(file.exists()){
    //          //删除文本
    //    //      file.delete()
    //          //删除文件夹
    //          FileUtils.deleteDirectory(file)
    //        }
    //向hdfs写
    //    val configuration = session.sparkContext.hadoopConfiguration

    //    val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop201:9000"), configuration, "root")
    //    val path: Path = new Path("/train/data1/ATPError")

    //    if (fs.exists(path)) {
    //      println("=====================================")
    //      fs.delete(path, true)
    //    }
    //    fs.close()

    //    val result: RDD[String] = resultRDD.map(tp => {
    //      val gson = new Gson()
    //      val jsonStr: String = gson.toJson(AttachRWBureau(tp._1, tp._2(0), tp._2(1), tp._2(2), tp._2(3), tp._2(4), tp._2(5), tp._2(6), tp._2(7), tp._2(8), tp._2(9)))
    //      jsonStr
    //    })
    //向本地保存
    //        result.saveAsTextFile("E:\\test\\train\\data\\jsonOutPutGson\\")
    //保存到hdfs
    //    result.saveAsTextFile("hdfs://hadoop201:9000/train/data1/ATPError/")

    //保存到数据库方式一：dataframe

    //    val props: Properties = new Properties()
    //    props.setProperty("driver",ConfigHelper.driver)
    //    props.setProperty("user",ConfigHelper.user)
    //    props.setProperty("password",ConfigHelper.password)
    //
    //    resultRDD.map(tp=>(tp._1, tp._2(0), tp._2(1), tp._2(2), tp._2(3), tp._2(4), tp._2(5), tp._2(6), tp._2(7), tp._2(8), tp._2(9)))
    //        .toDF("铁路局", "总数据量", "总报警次数", "车载主机", "无线传输单元", "应答器信息接收单元", "轨道电路信息读取器", "测速测距单元",
    //                "人机交互接口单元", "列车接口单元", "司法记录单元")
    //        .write.mode(SaveMode.Overwrite).jdbc(ConfigHelper.url,"ATPData1213",props)
    //        .show()

    DBs.setup()
    resultRDD.foreachPartition(partition => {
      DB.localTx { implicit session =>
        partition.foreach(tp => {
          SQL("insert into ATPData12132 values(?,?,?,?,?,?,?,?,?,?,?)")
            .bind(tp._1, tp._2(0), tp._2(1), tp._2(2), tp._2(3), tp._2(4), tp._2(5), tp._2(6), tp._2(7), tp._2(8), tp._2(9))
            .update()
            .apply()
        })
      }

      println("=========================================================================")
    })

    session.stop()

  }
}


case class AttachRWBureau(
                           铁路局: String,
                           总数据量: Int,
                           总报警次数: Int,
                           车载主机: Int,
                           无线传输单元: Int,
                           应答器信息接收单元: Int,
                           轨道电路信息读取器: Int,
                           测速测距单元: Int,
                           人机交互接口单元: Int,
                           列车接口单元: Int,
                           司法记录单元: Int
                         )
