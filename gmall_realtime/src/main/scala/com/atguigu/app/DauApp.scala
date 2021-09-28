package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}



object DauApp {
  def main(args: Array[String]): Unit = {
//    创建conf对象
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
//    创建ssc对象
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
//    调用kafkautill中的方法创建kafkadstream  用来消费kafka中的数据
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

//        测试kafka消费者及连接
////        kafkaDstream.foreachRDD(rdd=>{
////          rdd.foreachPartition(partition=>{
////            partition.foreach(record=>{
////              println(record.value())
////            })
////          })
////        })
    //   创建SimpleDateFormat类 将时间戳转换为固定时间格式
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
//   将kafka数据流转换为样例类
    val startUpLogDStream: DStream[StartUpLog] = kafkaDstream.mapPartitions(partition => {
      partition.map(record => {
        //        调用阿里JSON工具类中parseobject 方法将json字符串转换为样例类
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        //        补全样例类中的数据
        //        将样例类中的时间戳通过SimpleDateFormat类对象的format 方法转化日期格式
        val times: String = sdf.format(new Date(startUpLog.ts))
        startUpLog.logDate = times.split(" ")(0)
        startUpLog.logHour = times.split(" ")(1)
        //        返回样例类对象
        startUpLog
      })
    })
//    做缓存,避免重复掉用时重复计算
    startUpLogDStream.cache()
//    批次间去重(与redis中的数据进行比较)
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream,ssc.sparkContext)

    filterByRedisDStream.cache()

    //原始数据条数
    startUpLogDStream.count().print()

    //经过批次间去重后的数据条数
    filterByRedisDStream.count().print()
//    批次内去重
    DauHandler.filterByGroup()
//    将去重结果写入redis
    DauHandler.saveMidToRedis(filterByRedisDStream)

    ssc.start()
    //  阻塞任务
    ssc.awaitTermination()
  }
}

