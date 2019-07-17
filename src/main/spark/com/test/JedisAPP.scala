package com.test

import com.utils.{ConnectPoolUtils, JedisConnectionPool}
import org.apache.spark.rdd.RDD


/**
  * 指标统计
  */
object JedisAPP {
//指标一 1
  def Result01(lines: RDD[(String, List[Double])]): Unit = {
    lines.foreachPartition(f => {
      val jedis = JedisConnectionPool.getConnection()
      f.foreach(t => {
        // 充值订单数
        jedis.hincrBy(t._1, "count", t._2(0).toLong)
        // 充值金额
        jedis.hincrByFloat(t._1, "money", t._2(1))
        // 充值成功数
        jedis.hincrBy(t._1, "success", t._2(2).toLong)
        // 充值总时长
        jedis.hincrBy(t._1, "times", t._2(3).toLong)
      })
      jedis.close()
    })
  }
//指标一 2
  def Result02(lines: RDD[(String, Double)]): Unit = {
    lines.foreachPartition(f => {
      val jedis = JedisConnectionPool.getConnection()
      f.foreach(t => {
        jedis.incrBy(t._1, t._2.toLong)
      })
      jedis.close()
    })
  }
  //指标二
  def Result03(lines: RDD[((String, String), List[Double])]): Unit = {
    lines.foreachPartition(f=>{
      //获取连接
      val conn = ConnectPoolUtils.getConnections()
      //处理数据
      f.foreach(t=>{
        val sql = "insert into ProHour(Pro,Hour,counts)" +
        "values('"+t._1._1+"','"+t._1._2+"',"+(t._2(0)-t._2(2))+")"
        val state = conn.createStatement()
        state.executeUpdate(sql)
      })
      //还连接
      ConnectPoolUtils.resultConn(conn)
    })
  }
  //指标三
  def Result04(lines: RDD[(String , List[Double])]): Unit = {
    lines.foreachPartition(f=>{
      //获取连接
      val conn = ConnectPoolUtils.getConnections()
      //处理数据
      f.foreach(t=>{
        val sql = "insert into ProCounts(Pro,counts,feecounts,feepro)" +
          "values('"+t._1+"','"+t._2(0)+"',"+t._2(2)+","+t._2(2)/t._2(0)+")"
        val state = conn.createStatement()
        state.executeUpdate(sql)
      })
      //还连接
      ConnectPoolUtils.resultConn(conn)
    })
  }
  //指标四
  def Result05(lines:RDD[(String,List[Double])]): Unit ={
    lines.foreachPartition(f=>{
      val conn = ConnectPoolUtils.getConnections()
      f.foreach(f=>{
        val sql = "insert into HourCountMoney(Hour,counts,money)" +
        "values('"+f._1+"',"+f._2(2)+","+f._2(1)+")"
        val state = conn.createStatement()
        state.executeUpdate(sql)
      })
      //释放连接
      ConnectPoolUtils.resultConn(conn)
    })
  }
}