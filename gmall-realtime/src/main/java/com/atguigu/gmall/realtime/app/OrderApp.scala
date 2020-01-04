package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.constant.GmallConstant
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, OrderInfo}
import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OrderApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf , Seconds(5))
    ssc.sparkContext.setLogLevel("error")

    val inputDS = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER , ssc)

    val mapDS = inputDS.map(jsonStr => {
      val str = jsonStr.value()
      val orderInfo = JSON.parseObject(str, classOf[OrderInfo])

      val dateAndTime = orderInfo.create_time.split(" ")
      val time = dateAndTime(1).split(":")

      orderInfo.create_date = dateAndTime(0)
      orderInfo.create_hour = time(0)

      orderInfo.consignee_tel = orderInfo.consignee_tel.substring(0, 3) + "********"
      orderInfo
    })

    mapDS.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL2019_ORDER_INFO" , Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR") , new Configuration , Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
