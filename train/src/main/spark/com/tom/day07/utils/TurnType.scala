package com.tom.day07.utils

/**
 * ClassName: TurnType
 * Description: 
 *
 * @date 2019/12/17 19:34
 * @author Mi_dad
 */
object TurnType {
  def toInt(str: String) = {
    try {
      str.toInt
    } catch {
      case _: Exception => -1
    }
  }

  def toDouble(str: String) = {
    try {
      str.toDouble
    } catch {
      case _: Exception => -1.0
    }
  }

}
