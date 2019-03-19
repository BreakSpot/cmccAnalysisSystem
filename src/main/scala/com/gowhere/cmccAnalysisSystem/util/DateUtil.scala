package com.gowhere.cmccAnalysisSystem.util

import java.text.SimpleDateFormat

object DateUtil {
  def getDiffSeconds(time1:String, time2:String): Double = {
    //2017-04-12 03:00:20 010 320637069747919
    //2017-04-12 03:00:39 893
    val start = strhm2Date(time1.substring(0,17))
    val end = strhm2Date(time2)
    val sub = end-start
//    println(start)
//    println(end)
    sub*1.0/1000
  }

  /**
    * yyyyMMddHHmmss+hm字符串转时间戳
    * @param time
    * @return
    */
  def strhm2Date(time:String): Long ={
    val hm = time.substring(14,17).toLong
    val newtime :Long= new SimpleDateFormat("yyyyMMddHHmmss").parse(time.substring(0,14)).getTime
    newtime+hm
  }

  /**
    * 时间戳转字符串yyyy-MM-dd HH:mm:ss
    * @param time
    * @return
    */
  def Long2String(time:Long): String ={
    val newtime :String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time)
    newtime
  }
  /**
    * 字符串yyyyMMdd转yyyy-MM-dd
    * @param time
    * @return
    */
  def dateParedate(time:String): String ={
    time.substring(0,4)+"-"+time.substring(4,6)+"-"+time.substring(6,8)
  }
}
