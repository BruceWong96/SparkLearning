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

    //(url,urlname,uvid,ssid,sscount,sstime,cip)
    val d1 = stream.map{line =>
      val info = line.split("\\|")
      val url = info(0)
      val urlName = info(1)
      val uvid = info(13)
      val ssid = info(14).split("_")(0)
      val sscount = info(14).split("_")(1)
      val sstime = info(14).split("_")(2)
      val cip = info(15)
      (url, urlName, uvid, ssid, sscount, sstime, cip)
    }
    d1.print()

//    stream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
