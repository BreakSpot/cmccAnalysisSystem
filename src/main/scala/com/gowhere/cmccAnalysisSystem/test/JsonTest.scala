package com.gowhere.cmccAnalysisSystem.test

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object JsonTest {
  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getOrCreate("f:/bigdata/cmcc/out2", functionToCreateContext _)

    // 开始任务提交
    ssc.start()

    // 线程等待，等待处理下一批次任务
    ssc.awaitTermination()
  }

  def functionToCreateContext() = {
    val conf = new SparkConf().setAppName("streamingwc").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("f:/bigdata/cmcc/out2")
    // 获取NetCat服务端的数据
    val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop01", 6666)

    val tups = dStream.flatMap(_.split(" ")).map((_, 1))
    val res: DStream[(String, Int)] =
      tups.updateStateByKey(func, new HashPartitioner(3), true)

    // 实时的把结果返回到控制台
    res.print()

    ssc
  }
  val func = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map(x => {
      (x._1, x._2.sum + x._3.getOrElse(0))
    })
  }
}
