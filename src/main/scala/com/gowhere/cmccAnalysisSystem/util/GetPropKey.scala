package com.gowhere.cmccAnalysisSystem.util

import com.typesafe.config.{ConfigFactory, ConfigList}

object GetPropKey {
  val load = ConfigFactory.load()
  val topic = load.getString("kafka.topic")
  val brokers = load.getString("kafka.brokers")
  val jdbcurl = load.getString("jdbc.url")
  val jdbcuser = load.getString("jdbc.user")
  val jdbcpassword = load.getString("jdbc.password")
  val zkQuorum = load.getString("zookeeper.brokers")
  def getProvince() ={
    import scala.collection.JavaConversions._
    val provinceMap = load.getObject("province").unwrapped().toMap
    provinceMap
  }

}
