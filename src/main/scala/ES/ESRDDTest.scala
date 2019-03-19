package ES

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

object ESRDDTest {
  def main(args: Array[String]): Unit = {

    //模板代码
    val conf = new SparkConf()
      .setAppName("esrdd")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val arr = List((2019,1,2,6),(2019,1,3,4),(2019,1,4,6),(2019,2,1,3),(2019,2,6,6),(2019,2,6,9))
    val rdd = sc.makeRDD(arr).map(row=>{
      (row._2, (row._3, row._4))
    }).groupByKey()
    val res = rdd.map(x=>(x._1, x._2.toList.sortBy(_._2).reverse.take(2)))

    res.foreach(println)
    sc.stop()
  }
}
