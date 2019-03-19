package com.gowhere.cmccAnalysisSystem.app

import java.util

import com.gowhere.cmccAnalysisSystem.Jdbc.ScalalikeJdbcUtil
import com.gowhere.cmccAnalysisSystem.bean.{provincefailratio, provincehourfeeandnum, provinceminfeeandnum}
import com.gowhere.cmccAnalysisSystem.util.{DateUtil, GetPropKey}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ProvinceFailTop3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder().getOrCreate()


    val provinceBroadcast = sc.broadcast(GetPropKey.getProvince())
    val df = sparkSession.read.json("F:/bigdata/cmcc/cmcc.json")

    val reChargeNotifyReq = df.filter(_.getAs[String]("serviceName").equals("reChargeNotifyReq")).rdd
      .map(tuple=>{
        val bussinessRst = tuple.getAs[String]("bussinessRst")
        val requestId = tuple.getAs[String]("requestId")
        val date = DateUtil.dateParedate(requestId.substring(0, 8))
        val provinceCode = tuple.getAs[String]("provinceCode")
        val province = provinceBroadcast.value.getOrElse(provinceCode, "未知")
        val hour = requestId.substring(8, 10)
        val min = requestId.substring(10, 12)
        var chargefee = tuple.getAs[String]("chargefee").toInt
        val allnum = 1
        var failnum = 1
        if (bussinessRst.equals("0000")) {
          failnum = 0
        }
        //天，时，分，省份，金额，总数，失败数
        (date,hour, min, province.toString, chargefee, allnum, failnum)
      })
//    reChargeNotifyReq.take(20).foreach(println)

//    provinceFail(reChargeNotifyReq)
//
//    provinceMinFeeAndNum(reChargeNotifyReq)
//
    provinceHourFeeAndNum(reChargeNotifyReq)

    sc.stop()
  }
  def provinceHourFeeAndNum(reChargeNotifyReq: RDD[(String, String, String, String, Int, Int, Int)]): Unit ={
    val res = reChargeNotifyReq.filter(_._7==0).map(tuple=>{
      ((tuple._1, tuple._2, tuple._4),(tuple._5, tuple._6))
    }).reduceByKey((list1, list2)=>{
      (list1._1+list2._1, list1._2+list2._2)
    })
    res.take(20).foreach(println)
//    res.take(10).foreach(print)
//    res.foreachPartition(partithon=>{
//      val list = new util.ArrayList[provincehourfeeandnum]()
//      partithon.foreach(info=>{
//        list.add(provincehourfeeandnum(info._1._1, info._1._2,info._1._3,info._2._1, info._2._2))
//      })
//      ScalalikeJdbcUtil.provincehourandnumUpdateBatch(list)
//    })

  }

def provinceMinFeeAndNum(reChargeNotifyReq: RDD[(String, String, String, String, Int, Int, Int)]): Unit ={
  val res = reChargeNotifyReq.filter(_._7==0).map(tuple=>{
    ((tuple._1, tuple._2, tuple._3, tuple._4),(tuple._5, tuple._6))
  }).reduceByKey((list1, list2)=>{
    (list1._1+list2._1, list1._2+list2._2)
  })
  res.take(10).foreach(print)
  res.foreachPartition(partithon=>{
    val list = new util.ArrayList[provinceminfeeandnum]()
    partithon.foreach(info=>{
      list.add(provinceminfeeandnum(info._1._1, info._1._2,info._1._3,info._1._4,info._2._1, info._2._2))
    })
    ScalalikeJdbcUtil.provinceminfeeandnumUpdateBatch(list)
  })

}

  def provinceFail(reChargeNotifyReq: RDD[(String, String, String, String, Int, Int, Int)] ): Unit ={
    val provinceFailNum = reChargeNotifyReq.map(tuple=>{
      ((tuple._1, tuple._4),(tuple._6, tuple._7))
    })
    val res = provinceFailNum.reduceByKey((list1, list2)=>{
      (list1._1+list2._1, list1._2+list2._2)
    }).map(tuple=>{
      val radio = (tuple._2._2*1.0/tuple._2._1).formatted("%.2f").toDouble
      (tuple._1._1, tuple._1._2, tuple._2._2, radio)
    })
    res.take(30).foreach(print)
    res.foreachPartition(partithon=>{
      val list = new util.ArrayList[provincefailratio]()
      partithon.foreach(info=>{
        list.add(provincefailratio(info._1, info._2, info._3, info._4))
      })
      ScalalikeJdbcUtil.provincefailratioUpdateBatch(list)
    })
  }
}
