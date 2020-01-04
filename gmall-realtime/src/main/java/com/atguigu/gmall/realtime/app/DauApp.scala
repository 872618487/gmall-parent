package com.atguigu.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.constant.GmallConstant
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, RedisUtil, StartUpLog}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._

object DauApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc = new StreamingContext(sparkConf , Seconds(5))
    ssc.sparkContext.setLogLevel("error")

    //获取kafka数据
    val inputDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP , ssc)

    //转换成样例类 补充两个时间字段
    val logDS: DStream[StartUpLog] = inputDS.map {
      input => {
        val jsonStr = input.value()
        val startUpLog = JSON.parseObject(jsonStr, classOf[StartUpLog])

        val formatter = new SimpleDateFormat("yyyy-MM-dd HH")
        val timeStamp = new Date(startUpLog.ts)
        val date = formatter.format(timeStamp)

        val dateAndHour = date.split(" ")
        startUpLog.logDate = dateAndHour(0)
        startUpLog.logHour = dateAndHour(1)

        startUpLog
      }
    }

    //从Redis 中获取今天的数据，对比去重
    val distinctedDS: DStream[StartUpLog] = logDS.transform {
      rdd => {
        val formatter = new SimpleDateFormat("yyyy-MM-dd")
        val date: String = formatter.format(new Date())
        val jedis = RedisUtil.getJedisClient
        val todayUsers = jedis.smembers("dau:" + date)
        jedis.close()

        val usersBC = ssc.sparkContext.broadcast(todayUsers)
        println("过滤前：" + rdd.count())

        val filterRDD: RDD[StartUpLog] = rdd.filter {
          log => {
            val users = usersBC.value
            !users.contains(log.mid)
          }
        }
        println("过滤后：" + filterRDD.count())

        val groupByRDD: RDD[(String, Iterable[StartUpLog])] = filterRDD.groupBy(_.mid)

        val earlistLogRDD: RDD[(String, StartUpLog)] = groupByRDD.mapValues {
          logs => {
            val list = logs.toList
            list.sortWith {
              (left, right) => {
                left.ts < right.ts
              }
            }.take(1)
            list(0)
          }
        }
        println("过滤2后：" + earlistLogRDD.count())
        val result = earlistLogRDD.map(_._2)
        result
      }
    }

    distinctedDS.cache()

    distinctedDS.foreachRDD{
      rdd => {

        rdd.foreachPartition{
          logs => {
            val jedis = RedisUtil.getJedisClient
            logs.foreach{
              log => {
                jedis.sadd("dau:" + log.logDate , log.mid)
              }
            }
            jedis.close()
          }
        }
      }
    }

    distinctedDS.foreachRDD{
      rdd => {
          rdd.saveToPhoenix("GMALL2019_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS") ,new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
