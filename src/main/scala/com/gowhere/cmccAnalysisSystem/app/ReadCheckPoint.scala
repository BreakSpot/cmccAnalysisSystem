package com.gowhere.cmccAnalysisSystem.app

import java.util

import com.alibaba.fastjson.JSON
import com.gowhere.cmccAnalysisSystem.Jdbc.ScalalikeJdbcUtil
import com.gowhere.cmccAnalysisSystem.bean.{hourfeeandnum, provincefail, provincenumtop10}
import com.gowhere.cmccAnalysisSystem.util.{DateUtil, GetPropKey, JedisConnectionPool}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, TaskContext}

object ReadCheckPoint {

  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getOrCreate("f:/bigdata/cmcc/out", functionToCreateContext _)
    ssc.start()
    ssc.awaitTermination()
  }


  def functionToCreateContext() = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("f:/bigdata/cmcc/out")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop01:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "g007",
      "auto.offset.reset" -> "earliest", //latest"
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    val topics = Array("cmcctest")

    //从数据库中获取行偏移量
    val topicPartitionOffset = ScalalikeJdbcUtil.offsetCheck(topics)

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, topicPartitionOffset))

    val provinceBroadcast = sc.broadcast(GetPropKey.getProvince())

    val reChargeNotifyReq: DStream[(String, String, AnyRef, Double, Int, Int, Int, String, Int, Long)] = stream.map(rdd => (JSON.parseObject(rdd.value()), rdd))
      .filter(_._1.getString("serviceName").equals("reChargeNotifyReq"))
      .map(inputdstream => {
        val tuple = inputdstream._1
        val bussinessRst = tuple.getString("bussinessRst") //业务结果
        var chargefee = tuple.getString("chargefee").toInt //充值金额
        val requestId = tuple.getString("requestId")//时间
        val date = DateUtil.dateParedate(requestId.substring(0, 8))//日期
        val hour = requestId.substring(8, 10)//小时
        val receiveNotifyTime = tuple.getString("receiveNotifyTime")//结束时间
        var diffTime = DateUtil.getDiffSeconds(requestId, receiveNotifyTime)//时间差
        val provinceCode = tuple.getString("provinceCode")//地区
        val province = provinceBroadcast.value.getOrElse(provinceCode, "未知")//序列号转为文字
        var num = 1//交易都为成功
        if (!bussinessRst.equals("0000")) {//交易失败成功条件
          chargefee = 0
          num = 0
          diffTime = 0
        }
        //获取偏移量信息
        val topic = inputdstream._2.topic()
        val partition = inputdstream._2.partition()
        val offset = inputdstream._2.offset()

        (date, hour, province, diffTime, chargefee, num, 1, topic, partition, offset)
      }).cache()
    //统计全网的充值订单量, 充值金额, 充值成功数
    //dayTimeFeeNum(reChargeNotifyReq)
    //实时充值业务办理趋势, 主要统计全网的订单量数据
    //dayTimeFeeNum(reChargeNotifyReq)

    //统计每小时各个省份的充值失败数据量
    val provinceFail = reChargeNotifyReq.filter(_._6 == 0).map(tuple => {
      val data: String = tuple._1
      val hour: String = tuple._2
      val province = tuple._3.toString
      val TPO = tuple._8+tuple._9+tuple._10
      (TPO, data, hour, province, 1)
    })

    provinceFail.map(x=>{

    })
    //provinceFail.print()
    //dayTimeProvinceFail(provinceFail)

    //以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，只保留一位小数，存入MySQL中
    val provinceNum = reChargeNotifyReq.map(tuple => {
      val date = tuple._1
      val province = tuple._3.toString
      val success = tuple._6
      val num = tuple._7
      ((date, province), (success, num))
    }).updateStateByKey(provinceSum, new HashPartitioner(3), true)
    //    provinceNum.print()
        provinceNumTop10InputMysql(provinceNum)

    //实时统计每小时的充值笔数和充值金额
    val hourFeeAndNum = reChargeNotifyReq.filter(_._6 == 1).map(tuple => {
      val date = tuple._1
      val hour = tuple._2
      val fee = tuple._5
      val TPO = tuple._8+tuple._9+tuple._10
      (TPO, date, hour,1, fee)
    })
    hourFeeAndNum.print()
    hourFeeAndNumInputMysql(hourFeeAndNum)

    //更新offset

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o = offsetRanges(TaskContext.get.partitionId)
        ScalalikeJdbcUtil.updateOffset(o)
      }
    }
    ssc
  }


  val provinceSum = (it: (Iterator[((String, String), Seq[(Int, Int)], Option[(Int, Int)])])) => {
    it.map(x => {
      val suc = x._2.unzip._1.sum + x._3.getOrElse((0, 0))._1
      val num = x._2.unzip._2.sum + x._3.getOrElse((0, 0))._2
      ((x._1._1, x._1._2), (suc, num))
    })
  }

  val func = (it: (Iterator[((String, String, String), Seq[Int], Option[Int])])) => {
    it.map(x => {
      ((x._1._1, x._1._2, x._1._3), x._2.sum + x._3.getOrElse(0))
    })
  }

  //实时统计每小时的充值笔数和充值金额
  def hourFeeAndNumInputMysql(hourFeeAndNum: DStream[(String, String, String, Int, Int)]): Unit = {
    hourFeeAndNum.foreachRDD(partiton => {
      partiton.foreachPartition(tuple => {
        val list = new util.ArrayList[hourfeeandnum]()
        while (tuple.hasNext) {
          val info = tuple.next()
          list.add(hourfeeandnum(info._1, info._2, info._3, info._4, info._5))
        }
        ScalalikeJdbcUtil.hourfeeandnumUpdateBatch(list)
      })
    })
  }

  //以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，只保留一位小数，存入MySQL中
  def provinceNumTop10InputMysql(provinceNum: DStream[((String, String), (Int, Int))]): Unit = {
    val provinceNumTop10 = provinceNum.map(tuple => {
      val ratio = (tuple._2._1 * 1.0 / tuple._2._2).formatted("%.1f").toDouble
      (tuple._1._1, (tuple._1._2, tuple._2._2, ratio))
    }).groupByKey().map(row => {
      val sort = row._2.toList.sortBy(_._1).reverse.take(10)
      (row._1, sort)
    }).flatMap(tuple => {
      tuple._2.map(tp => (tuple._1, tp._1, tp._2, tp._3))
    })
    provinceNumTop10.foreachRDD(partiton => {
      partiton.foreachPartition(tuple => {
        val list = new util.ArrayList[provincenumtop10]()
        while (tuple.hasNext) {
          val info = tuple.next()
          list.add(provincenumtop10(info._1, info._2, info._3, info._4))
        }
        ScalalikeJdbcUtil.provincenumtop10UpdateBatch(list)
      })
    })
  }

  //统计每小时各个省份的充值失败数据量
  def dayTimeProvinceFail(provinceFail: DStream[(String, String, String, String, Int)]): Unit = {
    provinceFail.foreachRDD(partiton => {
      partiton.foreachPartition(tuple => {
        val list = new util.ArrayList[provincefail]()
        while (tuple.hasNext) {
          val info = tuple.next()
          list.add(provincefail(info._1, info._2, info._3, info._4, info._5))
        }
        ScalalikeJdbcUtil.provincefailUpdateBatch(list)
      })
    })
  }


  def dayTimeFeeNum(reChargeNotifyReq: DStream[(String, String, AnyRef, Double, Int, Int, Int, String, Int, Long)] ): Unit = {
    //统计全网的充值订单量, 充值金额, 充值成功数
    reChargeNotifyReq.map(tuple => {
      (tuple._1, (List(tuple._4, tuple._5, tuple._6, tuple._7)))
    }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(tp => tp._1 + tp._2)
    }).foreachRDD(partiton => {
      partiton.foreachPartition(tuple => {
        val jedis = JedisConnectionPool.getConnection()
        while (tuple.hasNext) {
          val info = tuple.next()
          jedis.hincrBy("one+" + info._1, "difftime", info._2(0).toLong)
          jedis.hincrBy("one+" + info._1, "fee", info._2(1).toLong)
          jedis.hincrBy("one+" + info._1, "successnum", info._2(2).toLong)
          jedis.hincrBy("one+" + info._1, "sum", info._2(3).toLong)
        }
        jedis.close()
      })
    })
  }

  def dateHourOrder(reChargeNotifyReq: DStream[(String, String, AnyRef, Double, Int, Int, Int, String, Int, Long)]): Unit = {
    //实时充值业务办理趋势, 主要统计全网的订单量数据
    reChargeNotifyReq.map(tuple => {
      ((tuple._1, tuple._2), (List(tuple._6, tuple._7)))
    }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(tp => tp._1 + tp._2)
    }).foreachRDD(partiton => {
      partiton.foreachPartition(tuple => {
        val jedis = JedisConnectionPool.getConnection()
        while (tuple.hasNext) {
          val info = tuple.next()
          jedis.hincrBy("two+" + info._1._1, info._1._2 + "successnum", info._2(0).toLong)
          jedis.hincrBy("two+" + info._1._1, info._1._2 + "sum", info._2(1).toLong)
        }
        jedis.close()
      })
    })
  }
}
