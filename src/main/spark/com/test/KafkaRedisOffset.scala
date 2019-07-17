package com.test

import java.lang

import com.alibaba.fastjson.JSON
import com.utils.{JUtils, JedisConnectionPool, JedisOffset}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Redis管理Offset
  */
object KafkaRedisOffset {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("offset").setMaster("local[2]")
      // 设置没秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition","100")
      // 设置序列化机制
      .set("spark.serlizer","org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf,Seconds(10))
    // 配置参数
    // 配置基本参数
    // 组名
    val groupId = "zk002"
    // topic
    val topic = "hz1803b"
    // 指定Kafka的broker地址（SparkStreaming程序消费过程中，需要和Kafka的分区对应）
    val brokerList = "192.168.203.100:9092"
    // 编写Kafka的配置参数
    val kafkas = Map[String,Object](
      "bootstrap.servers"->brokerList,
      // kafka的Key和values解码方式
      "key.deserializer"-> classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "group.id"->groupId,
      // 从头消费
      "auto.offset.reset"-> "earliest",
      // 不需要程序自动提交Offset
      "enable.auto.commit"-> (false:lang.Boolean)
    )
    // 创建topic集合，可能会消费多个Topic
    val topics = Set(topic)
    // 第一步获取Offset
    // 第二步通过Offset获取Kafka数据
    // 第三步提交更新Offset
    // 获取Offset
    var fromOffset:Map[TopicPartition,Long] = JedisOffset(groupId)
    // 判断一下有没数据
    val stream :InputDStream[ConsumerRecord[String,String]] =
      if(fromOffset.size == 0){
        KafkaUtils.createDirectStream(ssc,
          // 本地策略
          // 将数据均匀的分配到各个Executor上面
          LocationStrategies.PreferConsistent,
          // 消费者策略
          // 可以动态增加分区
          ConsumerStrategies.Subscribe[String,String](topics,kafkas)
        )
      }else{
        // 不是第一次消费
        KafkaUtils.createDirectStream(
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Assign[String,String](fromOffset.keys,kafkas,fromOffset)
        )
      }
    //读取省份文件
    val file = ssc.sparkContext.textFile("F://city.txt")
    //切分文件以map形式广播文件
    val map = file.map(t=>(t.split(" ")(0),t.split(" ")(1))).collect().toMap
    val broad = ssc.sparkContext.broadcast(map)
    stream.foreachRDD({
      rdd=>
        //获取更新后的偏移量
        val offestRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 业务处理
        val baseData = rdd.map(_.value()).map(t=>JSON.parseObject(t))
        //过滤需要的数据(充值通知)
          .filter(_.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
          .map(t=>{
            //先判断一下充值结果是否成功
            val result = t.getString("bussinessRst") //充值结果
            //充值金额
            val money:Double = if (result.equals("0000")) t.getDouble("chargefee") else 0.0
            //充值成功数
            val feecount = if (result.equals("0000")) 1 else 0
            //开始充值时间与结束充值时间
            val startTime = t.getString("requestId")
            val stopTime = t.getString("receiveNotifyTime")
            val jUtils = new JUtils
            //时间差 充值时长
            val times = jUtils.parseTime(stopTime) - jUtils.parseTime(startTime.substring(0,17))
            //省份编码
            val pro = t.getString("provinceCode")
            //获取省份名称
            val province = broad.value.get(pro).get
            (startTime.substring(0,8) //日期
              ,startTime.substring(0,12) //分钟
              ,startTime.substring(0,10) //小时
              ,List[Double](1,money,feecount,times)
              ,(province,startTime.substring(0,10))
              )
          }).cache()
        //指标一 1
        val result1 = baseData.map(t => (t._1, t._4)).reduceByKey((list1, list2) => {
          list1.zip(list2).map(t => t._1 + t._2)
        })
        JedisAPP.Result01(result1)
        //指标一 2
        val result2 = baseData.map(t=>(t._3,t._4.head)).reduceByKey(_+_)
        JedisAPP.Result02(result2)
        //指标二
        val result3 = baseData.map(t => ((t._5), t._4)).reduceByKey((list1, list2) => {
          list1.zip(list2).map(t => t._1 + t._2)
        })
        JedisAPP.Result03(result3)
        //指标三
        val result4 = baseData.map(t => (t._5._1, t._4)).reduceByKey((list1, list2) => {
          list1.zip(list2).map(t => t._1 + t._2)
        }).sortBy(_._2(0),false)
        JedisAPP.Result04(result4)
        //指标四
        val result5 = baseData.map(t => (t._3, t._4)).reduceByKey((list1, list2) => {
          list1.zip(list2).map(t => t._1 + t._2)
        })
        JedisAPP.Result05(result5)


        //处理第一个指标
        // 1)统计全网的充值订单量, 充值金额, 充值成功数,统计充值总时长。
        //2)实时充值业务办理趋势, 主要统计全网每分钟的订单量数据

        //二.统计每小时各个省份的充值失败数据量
        //三.以省份为维度统计订单量每个批次排名前 10 的省份数据,并且统计每个省份的订单成功率，
        // 只保留一位小数，存入MySQL中，进行前台页面显示。
        //四.实时统计每小时的充值笔数和充值金额。
        // 将偏移量进行更新
        val jedis = JedisConnectionPool.getConnection()
        for (or<-offestRange){
          jedis.hset(groupId,or.topic+"-"+or.partition,or.untilOffset.toString)
        }
        jedis.close()
    })
    // 启动
    ssc.start()
    ssc.awaitTermination()
  }
}
