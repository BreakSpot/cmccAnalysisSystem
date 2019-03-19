//package com.gowhere.cmccAnalysisSystem.util
//
//import kafka.common.TopicAndPartition
//import kafka.message.MessageAndMetadata
//import kafka.serializer.StringDecoder
//import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
//import org.I0Itec.zkclient.ZkClient
//import org.apache.spark.rdd.RDD
//import org.apache.spark.streaming.{Duration, StreamingContext}
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
//
//object OffsetUtil {
//  def fromOffsetsRanges(ssc: StreamingContext): InputDStream[(String, String)] ={
//
//
//    //指定组名
//    val group = "kafkaGroup"
//    //指定kafka的broker地址
//    val bootstrapServers = GetPropKey.brokers
//    //指定消费的 topic 名字
//    val topic = GetPropKey.topic
//    //指定zk的地址，后期更新消费的偏移量时使用(以后可以使用Redis、MySQL来记录偏移量)
//    val zkQuorum = GetPropKey.zkQuorum
//    //创建 stream 时使用的 topic 名字集合，SparkStreaming可同时消费多个topic
//    val topicsSet = Set(topic)
//
//    //创建一个 ZKGroupTopicDirs 对象,其实是指定往zk中写入数据的目录，用于保存偏移量
//    val topicDirs = new ZKGroupTopicDirs(group, topic)
//
//    //获取 zookeeper 中的路径
//    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"
//    //准备kafka的参数
//    val kafkaParams = Map[String, String]("metadata.broker.list" -> bootstrapServers,
//      "group.id" -> group,
//      //从头开始读取数据
//      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString)
//    //"auto.offset.reset" -> "largest")
//    // "auto.offset.reset" -> "smallest")
//
//    //是zookeeper的客户端，可以从zk中读取偏移量数据，并更新偏移量
//    val zkClient = new ZkClient(zkQuorum)
//
//    //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
//    val children = zkClient.countChildren(zkTopicPath)
//
//    var kafkaStream: InputDStream[(String, String)] = null
//
//
//    //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
//    var fromOffsets: Map[TopicAndPartition, Long] = Map()
//
//
//    //如果保存过 offset
//    if (children > 0) {
//      for (i <- 0 until children) {
//        val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")
//        val tp = TopicAndPartition(topic, i)
//        fromOffsets += (tp -> partitionOffset.toLong)
//      }
//      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())
//      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
//    } else {
//      //如果未保存，根据 kafkaParam 的配置使用最新(largest)或者最旧的（smallest） offset
//      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
//    }
//
//    //偏移量的范围
//    var offsetRanges = Array[OffsetRange]()
//
//    kafkaStream.foreachRDD { kafkaRDD =>
//      //只有KafkaRDD可以强转成HasOffsetRanges，并获取到偏移量
//      offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
//      val lines: RDD[String] = kafkaRDD.map(_._2)
//      val zkClient = new ZkClient(zkQuorum)
//      val topicDirs = new ZKGroupTopicDirs(group, topic)
////      //对RDD进行操作，触发Action
////      lines.foreachPartition(partition =>
////        partition.foreach(x => {
////          println(x)
////        })
////      )
//
//      for (o <- offsetRanges) {
//        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
//        ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
//      }
//    }
//
//   return kafkaStream
//  }
//}
