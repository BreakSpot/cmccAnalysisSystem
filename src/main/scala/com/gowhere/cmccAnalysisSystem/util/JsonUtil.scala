package com.gowhere.cmccAnalysisSystem.util

import com.alibaba.fastjson.JSON

object JsonUtil {
  def getJsonValue(str:String, key:String): String ={
    val json=JSON.parseObject(str)
    val value = json.getString(key)
    return value
  }

  def getServiceName(str:String):Int={
    val json=JSON.parseObject(str)
    val flag1 = json.containsKey("gateway_id")
    if(!flag1)
      return 3
    val flag2 = json.containsKey("rateoperateid")
    if(!flag2)
      return 2
    return 1
  }
}
