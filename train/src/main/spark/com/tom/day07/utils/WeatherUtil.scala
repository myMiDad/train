package com.tom.day07.utils

import org.apache.spark.sql.Row

/**
 * ClassName: WeatherUtil
 * Description: 
 *
 * @date 2019/12/17 19:32
 * @author Mi_dad
 */
object WeatherUtil {

  //获取温度等级
  def getTemLevel(row: Row) = {
    val tem: Int = TurnType.toInt(row.getAs[String]("Temperature"))
    if (tem >= -30 && tem <= -10) {
      "tem:-30~-10"
    } else if (tem > -10 && tem <= 0) {
      "tem:-10~0"
    } else if (tem > 0 && tem <= 10) {
      "tem:0~10"
    } else if (tem > 10 && tem <= 20) {
      "tem:10~20"
    } else if (tem > 20 && tem <= 30) {
      "tem:20~30"
    } else if (tem > 30 && tem <= 40) {
      "tem:30~40"
    } else if (tem > 40 && tem <= 50) {
      "tem:40~50"
    } else {
      "tem:数据不合法"
    }
  }

  //获取湿度等级
  def getHumLevel(row: Row) = {
    //获取湿度
    val hum: Int = TurnType.toInt(row.getAs[String]("Humidity"))
    //判断湿度等级
    if (hum > 0 && hum <= 20) {
      "hum:0~20"
    } else if (hum > 20 && hum <= 40) {
      "hum:20~40"
    } else if (hum > 40 && hum <= 60) {
      "hum:40~60"
    } else if (hum > 60 && hum <= 80) {
      "hum:60~80"
    } else if (hum > 80 && hum <= 100) {
      "hum:80~100"
    } else {
      "hum:数据不合法"
    }
  }

  //获取天气情况
  def getWeatherLevel(row: Row) = {
    //获取天气
    val wea = row.getAs[String]("Weather")
    if (wea.contains("雨")) {
      "wea:雨"
    } else if (wea.contains("雪")) {
      "wea:雪"
    } else if (wea.contains("冰雹")) {
      "wea:冰雹"
    } else if (wea.contains("浮沉")) {
      "wea:浮沉"
    } else if (wea.contains("扬沙")) {
      "wea:扬沙"
    } else if (wea.contains("晴天")) {
      "wea:晴天"
    } else {
      "wea:数据不合法"
    }
  }

  //获取速度等级
  def getSpeedLevel(row: Row) = {
    //获取速度
    val speed: Double = TurnType.toDouble(row.getAs[String]("MATPBaseInfo_Speed"))
    //判断速度等级
    if (speed >= 0 && speed <= 50) {
      "spe:0~50"
    } else if (speed >= 50 && speed <= 100) {
      "spe:50~100"
    } else if (speed >= 100 && speed <= 150) {
      "spe:100~150"
    } else if (speed >= 150 && speed <= 200) {
      "spe:150~200"
    } else if (speed >= 200 && speed <= 250) {
      "spe:200~250"
    } else if (speed >= 250 && speed <= 300) {
      "spe:250~300"
    } else if (speed >= 300 && speed <= 350) {
      "spe:300~350"
    } else if (speed >= 350 && speed <= 400) {
      "spe:350~400"
    } else {
      "spe:数据不合法"
    }
  }

}
