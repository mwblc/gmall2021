package com.atguigu.handler

import java.util

import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
  def filterByRedis(startUpLogDStream: DStream[StartUpLog]) ={
//    方案二  每个分区创建一次连接
    val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
      //      创建redis连接
      val jedis = new Jedis("hadoop105", 6379)
      //      调用filter函数
      val logs: Iterator[StartUpLog] = partition.filter(startUpLog => {
        //        查询redis中的数据
        //        定义key的类型
        val rediskey: String = "DAU:" + startUpLog.logDate
        val mids: util.Set[String] = jedis.smembers(rediskey)
        //3.将当前批次的mid去之前批次去重过后的mid（redis中查询出来的mid）做对比，重复的去掉
        val bool: Boolean = mids.contains(startUpLog.mid)
        !bool
      })
      jedis.close()
      logs
    })
    value
  }


  //  dstream  不能直接调用样例类中的属性,rdd算子可以,若调用时可以先调用rdd算子,
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]): Unit ={
    startUpLogDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
//        创建redis连接
        val jedis = new Jedis("hadoop105",6379)
        partition.foreach(startUpLog=>{
          val redisKey: String = "DAU:" + startUpLog.logDate
          jedis.sadd(redisKey,startUpLog.mid)
        })
//        关闭连接
        jedis.close()
      })
    })
  }


//  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]): Unit ={
//    startUpLogDStream.foreachRDD(rdd=>{
////      创建连接
//      val jedis = new Jedis("Hadoop105",6379)
//      rdd.foreach(startUpLog=>{
//
//      })
//      //      关闭连接
//      jedis.close()
//    })
//  }


}
