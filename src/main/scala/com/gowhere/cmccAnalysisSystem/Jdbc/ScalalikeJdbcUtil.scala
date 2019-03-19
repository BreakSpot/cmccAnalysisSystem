package com.gowhere.cmccAnalysisSystem.Jdbc

import java.util

import com.gowhere.cmccAnalysisSystem.bean.{hourfeeandnum, provincefail, provincefailratio, provincehourfeeandnum, provinceminfeeandnum, provincenumtop10}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}
import scala.collection.mutable


object ScalalikeJdbcUtil {

  /**
    * 城市交易失败数量
    * @param list
    */
  def provincefailUpdateBatch(list: util.ArrayList[provincefail]) = {
    DBs.setup()
    val iterable = list.iterator()
    while (iterable.hasNext) {
      val it = iterable.next()
      val s: Seq[List[String]] = DB.readOnly { implicit session =>
        SQL("select num from provincefail where date = ? and hour=? and province=?")
          .bind(it.date, it.hour, it.province)
          .map(rs => List(rs.string("num"))).list().apply()
      }
      val num = s.size
      if (num == 0) {
        //插入
        DB.autoCommit { implicit session =>
          SQL("insert into provincefail(TPO, date,hour,province,num) values(?,?,?,?,?)")
            .bind(it.TPO,it.date, it.hour, it.province, it.num).update().apply()
        }
      } else {
        //加上offset相关的列进行查询
        val s2: Seq[List[String]] = DB.readOnly { implicit session =>
          SQL("select num from provincefail where TPO=? and date = ? and hour=? and province=?")
            .bind(it.TPO, it.date, it.hour, it.province)
            .map(rs => List(rs.string("num"))).list().apply()
        }
        if(s2.size == 0){
          //更新
          DB.autoCommit { implicit session =>
            SQL("update provincefail set  TPO=? , num=num+? where date = ? and hour=? and province=?")
              .bind(it.TPO, it.num ,it.date, it.hour, it.province).update().apply()
          }
        }
      }
    }
  }

  /**
    * 每小时交易金额和数量
    * @param list
    */
  def hourfeeandnumUpdateBatch(list: util.ArrayList[hourfeeandnum]) = {
    DBs.setup()
    val iterable = list.iterator()
    while (iterable.hasNext) {
      val it = iterable.next()
      val s: Seq[List[String]] = DB.readOnly { implicit session =>
        SQL("select fee from hourfeeandnum where date = ? and hour=?").bind(it.date, it.hour)
          .map(rs => List(rs.string("fee"))).list().apply()
      }
      val num = s.size
      if (num == 0) {
        //插入
        DB.autoCommit { implicit session =>
          SQL("insert into hourfeeandnum(TPO,date,hour,num,fee) values(?,?,?,?,?)")
            .bind(it.TPO,it.date, it.hour, it.num, it.fee).update().apply()
        }
      } else {
        //加上offset相关的列进行查询
        val s2: Seq[List[String]] = DB.readOnly { implicit session =>
          SQL("select num from hourfeeandnum where TPO=? and date = ? and hour=?")
            .bind(it.TPO, it.date, it.hour)
            .map(rs => List(rs.string("num"))).list().apply()
        }
        if(s2.size == 0){
          //更新
          DB.autoCommit { implicit session =>
            SQL("update hourfeeandnum set TPO=?, num=num+?,fee=fee+? where date = ? and hour=?")
              .bind(it.TPO, it.num, it.fee, it.date, it.hour).update().apply()
          }
        }

      }
    }
  }

  /**
    * 城市交易TOP10
    * @param list
    */
  def provincenumtop10UpdateBatch(list: util.ArrayList[provincenumtop10]) = {
    DBs.setup()
    val iterable = list.iterator()

    DB.autoCommit { implicit session =>
      SQL("delete from provincenumtop10").update().apply()
    }
    while (iterable.hasNext) {
      val it = iterable.next()
      DB.autoCommit { implicit session =>
        SQL("insert into provincenumtop10(date,province,num, ratio) values(?,?,?,?)")
          .bind(it.date, it.province, it.num, it.ratio).update().apply()
      }
    }
  }

  /**
    * 城市交易失败数和概率
    * @param list
    */
  def provincefailratioUpdateBatch(list: util.ArrayList[provincefailratio]) = {
    DBs.setup()
    val iterable = list.iterator()
    while (iterable.hasNext) {
      val it = iterable.next()
      DB.autoCommit { implicit session =>
        SQL("insert into provincefailratio(date,province,num, ratio) values(?,?,?,?)")
          .bind(it.date, it.province, it.num, it.ratio).update().apply()
      }
    }
    DBs.close()
  }
  /**
    * 每分钟每省交易金额和交易数
    * @param list
    */
  def provinceminfeeandnumUpdateBatch(list: util.ArrayList[provinceminfeeandnum]) = {
    DBs.setup()
    val iterable = list.iterator()
    while (iterable.hasNext) {
      val it = iterable.next()
      DB.autoCommit { implicit session =>
        SQL("insert into provinceminfeeandnum(date,hour, min, province,fee, num) values(?,?,?,?,?,?)")
          .bind(it.date, it.hour, it.min,it.province, it.chargefee, it.allnum).update().apply()
      }
    }
  }

  /**
    * 每小时每省交易金额和交易数
    * @param list
    */
  def provincehourandnumUpdateBatch(list: util.ArrayList[provincehourfeeandnum]) = {
    DBs.setup()
    val iterable = list.iterator()
    while (iterable.hasNext) {
      val it = iterable.next()
      DB.autoCommit { implicit session =>
        SQL("insert into provincehourfeeandnum(date,hour, province,fee, num) values(?,?,?,?,?)")
          .bind(it.date, it.hour, it.province, it.chargefee, it.allnum).update().apply()
      }
    }
  }


  /**
    * 获取offset列表
    * @param topics
    * @return
    */
  def offsetCheck(topics: Array[String]): mutable.Map[TopicPartition, Long] ={
    DBs.setup()
    val res = DB.readOnly { implicit session =>
      val offsets = mutable.HashMap[TopicPartition, Long]()
      for(topic <- topics){
        SQL("select partition, offset from offset_check where topic=?")
          .bind(topic)
          .map(rs =>{
            val partition = rs.string("partition").toInt
            val offset = rs.string("offset").toLong
            offsets.put(new TopicPartition(topic, partition), offset)
          }).list().apply()
      }
      offsets
    }
    res
  }

  /**
    * 维护offset
    * @param it
    */
  def updateOffset(it: OffsetRange): Unit ={
    DBs.setup()
    DB.autoCommit { implicit session =>
      SQL("replace into offset_check set topic=?, partition=?, offset=?")
        .bind(it.topic, it.partition, it.untilOffset).update().apply()
    }
  }
}
