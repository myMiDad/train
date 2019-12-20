package com.tom.datasource

import com.tom.beans.LogSchema
import com.tom.config.ConfigHelper
import com.tom.utils.TurnType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 * ClassName: Data2Parquet
 * Description: 
 *
 * @date 2019/12/18 21:54
 * @author Mi_dad
 */
object Data2Parquet {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .config("spark.serializer", ConfigHelper.serializer)
      .config("spark.code", ConfigHelper.codec)
      .getOrCreate()

    import session.implicits._

    val source: Dataset[String] = session.read.textFile("file:///E:\\test\\ad\\data\\2016-10-01_06_p1_invalid.1475274123982.log")
    val sourceRDD: RDD[Array[String]] = source.map(_.split(",", -1)).filter(_.length >= 85).rdd.cache()
    val sourceRow: RDD[Row] = sourceRDD.map(arr => Row(
      arr(0),
      TurnType.toInt(arr(1)),
      TurnType.toInt(arr(2)),
      TurnType.toInt(arr(3)),
      TurnType.toInt(arr(4)),
      arr(5),
      arr(6),
      TurnType.toInt(arr(7)),
      TurnType.toInt(arr(8)),
      TurnType.toDouble(arr(9)),
      TurnType.toDouble(arr(10)),
      arr(11),
      arr(12),
      arr(13),
      arr(14),
      arr(15),
      arr(16),
      TurnType.toInt(arr(17)),
      arr(18),
      arr(19),
      TurnType.toInt(arr(20)),
      TurnType.toInt(arr(21)),
      arr(22),
      arr(23),
      arr(24),
      arr(25),
      TurnType.toInt(arr(26)),
      arr(27),
      TurnType.toInt(arr(28)),
      arr(29),
      TurnType.toInt(arr(30)),
      TurnType.toInt(arr(31)),
      TurnType.toInt(arr(32)),
      arr(33),
      TurnType.toInt(arr(34)),
      TurnType.toInt(arr(35)),
      TurnType.toInt(arr(36)),
      arr(37),
      TurnType.toInt(arr(38)),
      TurnType.toInt(arr(39)),
      TurnType.toDouble(arr(40)),
      TurnType.toDouble(arr(41)),
      TurnType.toInt(arr(42)),
      arr(43),
      TurnType.toDouble(arr(44)),
      TurnType.toDouble(arr(45)),
      arr(46),
      arr(47),
      arr(48),
      arr(49),
      arr(50),
      arr(51),
      arr(52),
      arr(53),
      arr(54),
      arr(55),
      arr(56),
      TurnType.toInt(arr(57)),
      TurnType.toDouble(arr(58)),
      TurnType.toInt(arr(59)),
      TurnType.toInt(arr(60)),
      arr(61),
      arr(62),
      arr(63),
      arr(64),
      arr(65),
      arr(66),
      arr(67),
      arr(68),
      arr(69),
      arr(70),
      arr(71),
      arr(72),
      TurnType.toInt(arr(73)),
      TurnType.toDouble(arr(74)),
      TurnType.toDouble(arr(75)),
      TurnType.toDouble(arr(76)),
      TurnType.toDouble(arr(77)),
      TurnType.toDouble(arr(78)),
      arr(79),
      arr(80),
      arr(81),
      arr(82),
      arr(83),
      TurnType.toInt(arr(84))
    ))
    val frame: DataFrame = session.createDataFrame(sourceRow, LogSchema.schema)
//        frame.show(10000)

//    frame.coalesce(1).write.mode(SaveMode.Overwrite).parquet("E:\\test\\ad\\parquent")
    frame.write.mode(SaveMode.Overwrite).partitionBy("provincename","cityname").parquet("E:\\test\\ad\\parquent1")
    session.stop()
  }
}
