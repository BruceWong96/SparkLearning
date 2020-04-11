package com.weblog

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("weblog")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val zkHosts = "hadoop01:2181,hadoop02:2181,hadoop03:2181"
    val group = "group2"
    val topics = Map("weblog"->1)
    val stream = KafkaUtils.createStream(ssc, zkHosts, group, topics)
      .map(_._2)

    stream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
