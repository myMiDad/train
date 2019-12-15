package com.tom.code

import java.text.SimpleDateFormat
import java.util.Date

import com.tom.beans.logSchema
import com.tom.config.ConfigHelper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
 * ClassName: Test
 * Description:
 *
 * @date 2019/12/12 10:48
 * @author Mi_dad
 */
object DataKafka {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .config("spark.serializer",ConfigHelper.serializer)
      .config("spark.sql.parquet.compression.codec",ConfigHelper.codec)
      .getOrCreate()

    import session.implicits._

    //获取数据
//    val source: Dataset[String] = session.read.textFile("hdfs://hadoop201:9000/train/kafkaData/")
    val source: Dataset[String] = session.read.textFile("file:///E:/test/train/data/高铁数据.txt")

    //切分过滤有效数据
    val splits: Dataset[Array[String]] = source.map(_.split("\\|",-1)).filter(_.length>=55)
    val sourceRDD: RDD[Array[String]] = splits.rdd.cache()
    //需求一：300T+300TATO
    //获取当前时间
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val date = new Date()
    //注意：代码中尽量少使用filter，效率较低
    val filter300T: RDD[Array[String]] = sourceRDD.filter(arr => arr(0).startsWith("300T"))
      .filter(arr => !arr(17).contains("复位") || !arr(17).contains("SB待机") || !arr(17).contains("制动报警"))
      .filter(arr => (date.getTime - format.parse(arr(7)).getTime) <= 300000000000000000L)
      .filter(arr => !arr(17).contains("休眠") || !arr(17).contains("未知") || !arr(17).contains("无致命错误") || !arr(17).contains("一致性消息错误") || !arr(17).contains("NVMEM故障")).cache()

    val filter300T2: RDD[Array[String]] = filter300T.filter(arr => arr(17).contains("当前ATP系统处于故障中[SF]"))
      .map(arr => {
        arr(0) + "|" +
        arr(1) + "|" +
        arr(2) + "|" +
        arr(3) + "|" +
        arr(4) + "|" +
        arr(5) + "|" +
        arr(6) + "|" +
        arr(7).substring(1, 12) + "00|" +
        arr(8) + "|" +
        arr(9) + "|" +
        arr(10) + "|" +
        arr(11) + "|" +
        arr(12) + "|" +
        arr(13) + "|" +
        arr(14) + "|" +
        arr(15) + "|" +
        arr(16) + "|" +
        arr(17) + "|" +
        arr(18) + "|" +
        arr(19) + "|" +
        arr(20) + "|" +
        arr(21) + "|" +
        arr(22) + "|" +
        arr(23) + "|" +
        arr(24) + "|" +
        arr(25) + "|" +
        arr(26) + "|" +
        arr(27) + "|" +
        arr(28) + "|" +
        arr(29) + "|" +
        arr(30) + "|" +
        arr(31) + "|" +
        arr(32) + "|" +
        arr(33) + "|" +
        arr(34) + "|" +
        arr(35) + "|" +
        arr(36) + "|" +
        arr(37) + "|" +
        arr(38) + "|" +
        arr(39) + "|" +
        arr(40) + "|" +
        arr(41) + "|" +
        arr(42) + "|" +
        arr(43) + "|" +
        arr(44) + "|" +
        arr(45) + "|" +
        arr(46) + "|" +
        arr(47) + "|" +
        arr(48) + "|" +
        arr(49) + "|" +
        arr(50) + "|" +
        arr(51) + "|" +
        arr(52) + "|" +
        arr(53) + "|" +
        arr(54)
    }).distinct().map(_.split("\\|", -1))
    //需求：两条相近的故障记录中包括“当前ATP系统处于故障中[SF]”时删除该条记录，单独时不删除这一条
    //实现：先过滤出[当前ATP系统处于故障中[SF]的记录，然后将每条数据转换为字符串，并将同一分钟内的时间秒数归零，认为相近记录


    //需求二：300S+300SATO
    val filter300S: RDD[Array[String]] = sourceRDD.filter(arr => arr(0).startsWith("300S"))
      .filter(arr => !arr(17).contains("休眠") && !(arr(9).contains("CTCS-3") && arr(17).contains("SB")) || !arr(17).contains("切换到备系"))

    //需求三：200H
    val filter200H: RDD[Array[String]] = sourceRDD.filter(arr=> arr(0).equals("200H") && !arr(17).contains("休眠") && !arr(17).contains("VC2"))

    //需求四：300H
    val filter300H: RDD[Array[String]] = sourceRDD.filter(arr=>arr(0).equals("300H") && arr(17).contains("休眠"))


    //将所有的过滤完的数据进行取并集
    val result: RDD[Array[String]] = filter300T.filter(arr => !arr(17).contains("当前ATP系统处于故障中[SF]"))
      .union(filter300T2)
      .union(filter300S)
      .union(filter200H)
      .union(filter300H)
      .union(sourceRDD.filter(arr => !arr(0).startsWith("300T") && !arr(0).startsWith("300S") && !arr(0).equals("300H") && !arr(0).equals("200H")))

//    result.foreach(println)

    val rddRow: RDD[Row] = result.map(arr => Row(
      arr(0),
      arr(1),
      arr(2),
      arr(3),
      arr(4),
      arr(5),
      arr(6),
      arr(7),
      arr(8),
      arr(9),
      arr(10),
      arr(11),
      arr(12),
      arr(13),
      arr(14),
      arr(15),
      arr(16),
      arr(17),
      arr(18),
      arr(19),
      arr(20),
      arr(21),
      arr(22),
      arr(23),
      arr(24),
      arr(25),
      arr(26),
      arr(27),
      arr(28),
      arr(29),
      arr(30),
      arr(31),
      arr(32),
      arr(33),
      arr(34),
      arr(35),
      arr(36),
      arr(37),
      arr(38),
      arr(39),
      arr(40),
      arr(41),
      arr(42),
      arr(43),
      arr(44),
      arr(45),
      arr(46),
      arr(47),
      arr(48),
      arr(49),
      arr(50),
      arr(51),
      arr(52),
      arr(53),
      arr(54)
    ))
    //将数据转换成parquet文件
    val frame: DataFrame = session.createDataFrame(rddRow,logSchema.schema)
    frame.write.mode(SaveMode.Overwrite).partitionBy("MPacketHead_ATPType").parquet("E:/test/train/data/parquetData")

//    frame.coalesce(1).write.mode(SaveMode.Overwrite).parquet("E:/test/train/data/parquetData1")
    //释放资源
    session.stop()
  }

}
