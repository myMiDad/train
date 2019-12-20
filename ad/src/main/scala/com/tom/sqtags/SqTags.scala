package com.tom.sqtags

import java.net.URL
import java.nio.charset.StandardCharsets

import org.apache.http.HttpEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import sun.net.www.http.HttpClient

/**
 * ClassName: SqTags
 * Description: 
 *
 * @date 2019/12/19 22:32
 * @author Mi_dad
 */
object SqTags {
  def main(args: Array[String]): Unit = {
    val str = "http://api.map.baidu.com/reverse_geocoding/v3/?ak=NawBqTCmQzMzoSggmvOouIPclb5rGYIF&output=json&coordtype=wgs84ll&location=31.225696563611,121.49884033194"
    val httpClient = HttpClientBuilder.create().build()
    val httpGet = new HttpGet(str)
    val response: CloseableHttpResponse = httpClient.execute(httpGet)
    val entity = response.getEntity
    val line: String = EntityUtils.toString(entity, StandardCharsets.UTF_8)
    println(line)

  }

}
