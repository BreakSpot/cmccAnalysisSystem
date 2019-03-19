package com.gowhere.cmccAnalysisSystem.util

import com.typesafe.config.ConfigFactory

object GetPropKey {
  val load = ConfigFactory.load()
  val topic = load.getString("kafka.topic")
  val brokers = load.getString("kafka.brokers")
}
