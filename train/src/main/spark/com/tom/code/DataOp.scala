package com.tom.code

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * ClassName: DataOp
 * Description: 
 *
 * @date 2019/12/12 10:18
 * @author Mi_dad
 */
object DataOp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(1))

    val source: DStream[String] = ssc.textFileStream("hdfs://hadoop201:9000/train/kafkaData/")

    source.foreachRDD(rdd=>{
      rdd.foreach(println(_))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
