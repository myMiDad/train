package com.tom.day06.sparkstreaming.utils

import com.tom.config.ConfigHelper
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

/**
 * ClassName: OffSetUtil
 * Description: 
 *
 * @date 2019/12/16 17:17
 * @author Mi_dad
 */
object OffSetUtil {
  //读取mysql中的偏移量
  def readOffSet()={
    //加载配置文件
    DBs.setup()
    val map: Map[TopicPartition, Long] = DB.readOnly { implicit session =>
      SQL("select * from offset where topic = ? and groupid = ?")
        .bind(ConfigHelper.topics(0), ConfigHelper.groupid)
        .map(line => (
          new TopicPartition(line.string("topic"), line.int("partition")),
          line.long("offset")
        )).list().apply()
    }.toMap
    map
  }
  //将偏移量存储到mysql
  def saveOffSet(offSetRange:Array[OffsetRange])={
    //加载配置文件
    DBs.setup()
    DB.localTx{implicit session=>
      offSetRange.foreach(offset=>{
        SQL("update offset set offset=? where topic=? and partition=? and groupid=?")
          .bind(offset.untilOffset,offset.topic,offset.partition,ConfigHelper.groupid)
          .update()
          .apply()
      })
    }
  }

}
