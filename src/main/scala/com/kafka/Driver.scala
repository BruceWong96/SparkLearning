package com.kafka

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Driver {
  def main(args: Array[String]): Unit = {

    //SparkStreaming 从Kafka消费数据时,启动的线程数至少是2个
    //指定方式local[线程数] 其中一个线程用于启动和监听Streaming
    //另一个线程用于消费
    //--如果写成local,表示只有一个线程,会导致streaming可以启动,但是没有线程去消费数据
    val conf = new SparkConf().setMaster("local[2]").setAppName("kafkaInput")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val zkHosts = "hadoop01:2181,hadoop02:2181,hadoop03:2181"
    val group = "group1"

    //用Map结构来制定消费的主题名,以及消费的线程数
    val topics = Map("enbook"->1)

    //通过Kafka获取数据源
    //通过(null, 数据)
    val stream = KafkaUtils.createStream(ssc, zkHosts, group, topics).map(_._2)

    stream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
