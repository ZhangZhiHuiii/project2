package com.utils


import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD


/**
  * 方法类
  */
class JUtils extends Serializable {
  def parseTime(str:String): Long ={
    val fm = new SimpleDateFormat("yyyyMMddhhmmssSSS")
    val dt: Date = fm.parse(str)
//    val aa: String = fm.format(dt)
    val tim: Long = dt.getTime
    tim
  }

}
//object Test9{
//  def main(args: Array[String]): Unit = {
//    val str = "19970718050505"
//    val jUtils = new JUtils
//    println(jUtils.parseTime(str))
//  }
//}