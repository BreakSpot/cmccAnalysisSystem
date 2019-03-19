//package com.gowhere.cmccAnalysisSystem.app
//
//
//import com.alibaba.fastjson.JSON
//import com.gowhere.cmccAnalysisSystem.util._
//import kafka.serializer.StringDecoder
//import java.util
//
//import com.gowhere.cmccAnalysisSystem.Jdbc.ScalalikeJdbcUtil
//import com.gowhere.cmccAnalysisSystem.bean.{hourfeeandnum, provincefail, provincenumtop10}
//import kafka.common.TopicAndPartition
//import kafka.message.MessageAndMetadata
//import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
//import org.I0Itec.zkclient.ZkClient
//import org.apache.spark.rdd.RDD
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
//
//
//object preducer {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
//    val sc = new SparkContext(conf)
//    val ssc = new StreamingContext(sc, Seconds(5))
//    ssc.checkpoint("F:/bigdata/cmcc/checklog")
//
//    val cmccJson = OffsetUtil.fromOffsetsRanges(ssc).map(_._2)
//    //订单量，充值金额，成功率，平均时长
//    //{"bussinessRst":"0000","channelCode":"0705","chargefee":"1000","clientIp":"125.82.118.3","gateway_id":"CMPAY","idType":"01",
//    // "interFacRst":"0000","logOutTime":"20170412030039946","orderId":"384663620178010331","phoneno":"18384144665","provinceCode":"280",
//    // "receiveNotifyTime":"20170412030039893","requestId":"20170412030020010320637069747919","resultTime":"20170412030039",
//    // "retMsg":"接口调用成功","serverIp":"10.255.254.10","serverPort":"8714","serviceName":"reChargeNotifyReq","sysId":"15"}
//
//    val provinceBroadcast = sc.broadcast(GetPropKey.getProvince())
//    //数据过滤清洗
//    val reChargeNotifyReq = cmccJson.map(rdd => JSON.parseObject(rdd))
//      .filter(_.getString("serviceName").equals("reChargeNotifyReq"))
//      .map(tuple => {
//        val bussinessRst = tuple.getString("bussinessRst") //业务结果
//        var chargefee = tuple.getString("chargefee").toInt //充值金额
//        val requestId = tuple.getString("requestId")
//        //2017-04-12 03:00:20 010320637069747919
//        val date = DateUtil.dateParedate(requestId.substring(0, 8))
//        val hour = requestId.substring(8, 10)
//        val min = requestId.substring(10, 12)
//        val receiveNotifyTime = tuple.getString("receiveNotifyTime")
//        //2017-04-12 03:00:39 893
//        var diffTime = DateUtil.getDiffSeconds(requestId, receiveNotifyTime)
//        val provinceCode = tuple.getString("provinceCode")
//        val province = provinceBroadcast.value.getOrElse(provinceCode, "未知")
//        var num = 1
//        if (!bussinessRst.equals("0000")) {
//          chargefee = 0
//          num = 0
//          diffTime = 0
//        }
//        (date, hour, min, province, diffTime, chargefee, num, 1)
//      })
//    //    reChargeNotifyReq.print()
//
//    //统计全网的充值订单量, 充值金额, 充值成功数
//    dayTimeFeeNum(reChargeNotifyReq)
//    //实时充值业务办理趋势, 主要统计全网的订单量数据
//    dayTimeFeeNum(reChargeNotifyReq)
//
//
//    //统计每小时各个省份的充值失败数据量
//    val provinceFail = reChargeNotifyReq.filter(_._7 == 0).map(tuple => {
//      val data: String = tuple._1
//      val hour: String = tuple._2
//      val province = tuple._4.toString
//      ((data, hour, province), 1)
//    })
//    provinceFail.print()
//    dayTimeProvinceFail(provinceFail)
//
//    //以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，只保留一位小数，存入MySQL中
//    val provinceNum = reChargeNotifyReq.map(tuple => {
//      val date = tuple._1
//      val province = tuple._4.toString
//      val success = tuple._7
//      val num = tuple._8
//      ((date, province), (success, num))
//    }).updateStateByKey(provinceSum, new HashPartitioner(3), true)
//    provinceNum.print()
//    provinceNumTop10InputMysql(provinceNum)
//
//    //实时统计每小时的充值笔数和充值金额
//    val hourFeeAndNum = reChargeNotifyReq.filter(_._7 == 1).map(tuple => {
//      val date = tuple._1
//      val hour = tuple._2
//      val fee = tuple._6
//      ((date, hour), (1, fee))
//    })
//    hourFeeAndNum.print()
//    hourFeeAndNumInputMysql(hourFeeAndNum)
//
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
//  val provinceSum = (it: (Iterator[((String, String), Seq[(Int, Int)], Option[(Int, Int)])])) => {
//    it.map(x => {
//      val suc = x._2.unzip._1.sum + x._3.getOrElse((0, 0))._1
//      val num = x._2.unzip._2.sum + x._3.getOrElse((0, 0))._2
//      ((x._1._1, x._1._2), (suc, num))
//    })
//  }
//
//  val func = (it: (Iterator[((String, String, String), Seq[Int], Option[Int])])) => {
//    it.map(x => {
//      ((x._1._1, x._1._2, x._1._3), x._2.sum + x._3.getOrElse(0))
//    })
//  }
//
//  //实时统计每小时的充值笔数和充值金额
//  def hourFeeAndNumInputMysql(hourFeeAndNum: DStream[((String, String), (Int, Int))]): Unit ={
//    hourFeeAndNum.map(tuple=>{
//      (tuple._1._1, tuple._1._2, tuple._2._1, tuple._2._2.toLong)
//    }).foreachRDD(partiton => {
//      partiton.foreachPartition(tuple=>{
//        val list = new util.ArrayList[hourfeeandnum]()
//        while(tuple.hasNext){
//          val info = tuple.next()
//          list.add(hourfeeandnum(info._1, info._2, info._3, info._4))
//        }
//        ScalalikeJdbcUtil.hourfeeandnumUpdateBatch(list)
//      })
//
//    })
//  }
//
//  //以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，只保留一位小数，存入MySQL中
//  def provinceNumTop10InputMysql(provinceNum: DStream[((String, String), (Int, Int))]): Unit ={
//    val provinceNumTop10 = provinceNum.map(tuple => {
//      val ratio = (tuple._2._1 * 1.0 / tuple._2._2).formatted("%.1f").toDouble
//      (tuple._1._1, (tuple._1._2, tuple._2._2, ratio))
//    }).groupByKey().map(row => {
//      val sort = row._2.toList.sortBy(_._1).reverse.take(10)
//      (row._1, sort)
//    }).flatMap(tuple => {
//      tuple._2.map(tp => (tuple._1, tp._1, tp._2, tp._3))
//    })
//    provinceNumTop10.foreachRDD(partiton => {
//      partiton.foreachPartition(tuple=>{
//        val list = new util.ArrayList[provincenumtop10]()
//        while(tuple.hasNext){
//          val info = tuple.next()
//          list.add(provincenumtop10(info._1, info._2, info._3, info._4))
//        }
//        ScalalikeJdbcUtil.provincenumtop10UpdateBatch(list)
//      })
//    })
//  }
//  //统计每小时各个省份的充值失败数据量
//  def dayTimeProvinceFail(provinceFail: DStream[((String, String, String), Int)]): Unit = {
//    provinceFail.map(tuple => {
//      val data = tuple._1._1
//      val hour = tuple._1._2
//      val province = tuple._1._3
//      val num = tuple._2
//      (data, hour, province, num)
//    }).foreachRDD(partiton => {
//      partiton.foreachPartition(tuple=>{
//        val list = new util.ArrayList[provincefail]()
//        while(tuple.hasNext){
//          val info = tuple.next()
//          list.add(provincefail(info._1, info._2, info._3, info._4))
//        }
//        ScalalikeJdbcUtil.provincefailUpdateBatch(list)
//      })
//    })
//
//  }
//
//
//  def dayTimeFeeNum(reChargeNotifyReq: DStream[(String, String, String, AnyRef, Double, Int, Int, Int)]): Unit = {
//    //统计全网的充值订单量, 充值金额, 充值成功数
//    reChargeNotifyReq.map(tuple => {
//      (tuple._1, (List(tuple._5, tuple._6, tuple._7, tuple._8)))
//    }).reduceByKey((list1, list2) => {
//      list1.zip(list2).map(tp => tp._1 + tp._2)
//    }).foreachRDD(partiton => {
//      partiton.foreachPartition(tuple => {
//        val jedis = JedisConnectionPool.getConnection()
//        while (tuple.hasNext) {
//          val info = tuple.next()
//          jedis.hincrBy("one+" + info._1, "difftime", info._2(0).toLong)
//          jedis.hincrBy("one+" + info._1, "fee", info._2(1).toLong)
//          jedis.hincrBy("one+" + info._1, "successnum", info._2(2).toLong)
//          jedis.hincrBy("one+" + info._1, "sum", info._2(3).toLong)
//        }
//        jedis.close()
//      })
//    })
//  }
//
//  def dateHourOrder(reChargeNotifyReq: DStream[(String, String, String, String, Double, Int, Int, Int)]): Unit = {
//    //实时充值业务办理趋势, 主要统计全网的订单量数据
//    reChargeNotifyReq.map(tuple => {
//      ((tuple._1, tuple._2), (List(tuple._7, tuple._8)))
//    }).reduceByKey((list1, list2) => {
//      list1.zip(list2).map(tp => tp._1 + tp._2)
//    }).foreachRDD(partiton => {
//      partiton.foreachPartition(tuple => {
//        val jedis = JedisConnectionPool.getConnection()
//        while (tuple.hasNext) {
//          val info = tuple.next()
//          jedis.hincrBy("two+" + info._1._1, info._1._2 + "successnum", info._2(0).toLong)
//          jedis.hincrBy("two+" + info._1._1, info._1._2 + "sum", info._2(1).toLong)
//        }
//        jedis.close()
//      })
//    })
//  }
//}
