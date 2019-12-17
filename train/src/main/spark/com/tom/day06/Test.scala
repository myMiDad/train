package com.tom.day06

import redis.clients.jedis.Jedis

/**
 * ClassName: Test
 * Description: 
 *
 * @date 2019/12/16 11:06
 * @author Mi_dad
 */
object Test {
  def main(args: Array[String]): Unit = {
    val jedis = new Jedis("hadoop201",6379)
    print(jedis.hget("trainDate","2009084"))
  }

}
