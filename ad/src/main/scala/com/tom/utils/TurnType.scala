package com.tom.utils

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
  def integer2Int(integer: Integer) = {
    try {
      integer.toInt
    } catch {
      case _: Exception => 0
    }
  }

}
