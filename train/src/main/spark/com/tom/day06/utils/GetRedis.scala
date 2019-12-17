package com.tom.day06.utils

import redis.clients.jedis.{Jedis, JedisPool}

/**
 * ClassName: GetRedis
 * Description: 
 *
 * @date 2019/12/16 15:46
 * @author Mi_dad
 */
object GetRedis {
  private lazy val pool = new JedisPool("hadoop201", 6379)

  def getRedisConn(index: Int = 0) = {
    val jedis: Jedis = pool.getResource
    jedis.select(index)
    jedis
  }

}
