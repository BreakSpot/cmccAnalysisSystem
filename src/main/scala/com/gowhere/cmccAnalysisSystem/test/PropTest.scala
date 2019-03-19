package com.gowhere.cmccAnalysisSystem.test

import com.gowhere.cmccAnalysisSystem.util.GetPropKey
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PropTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
    val sc = new SparkContext(conf)

    val list = List(("x",1,2,3), ("x", 1,2,3), ("y", 1,2,3), ("x", 1,2,3))
    val list2 = List(1,2,3)
    val list4 = List(4,5,6)
    println(list2.zip(list4))
    val unit: RDD[((String, String), List[(Int, Int, Int)])] = sc.makeRDD(list).map(tuple => {
      ((tuple._1, "cc"), List((tuple._2, tuple._3, tuple._4)))
    }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(tp => {
        (tp._1._1 + tp._2._1, tp._1._2 + tp._2._2, tp._1._3 + tp._2._3)
      })
    })
    print(unit)

  }
}
