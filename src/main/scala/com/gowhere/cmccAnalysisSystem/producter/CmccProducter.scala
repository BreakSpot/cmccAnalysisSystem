package com.gowhere.cmccAnalysisSystem.producter

import java.util
import com.gowhere.cmccAnalysisSystem.util.GetPropKey
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import scala.io.Source

object CmccProducter {
  def main(args: Array[String]): Unit = {

    val fileName = "F:/bigdata/cmcc/cmcc.json"

    val fileData = Source.fromFile(fileName)

    val (brokers, topic) = (GetPropKey.brokers, GetPropKey.topic)
    // Zookeeper connection properties
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    for(line <- fileData.getLines)
    {
      val message = line
      producer.send(new ProducerRecord[String, String](topic, message))
      println(line)
      Thread.sleep(50)
    }
    fileData.close()
  }
}
