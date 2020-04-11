package com.stream

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("stream")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    //设置检查点数据的存储目录
    ssc.checkpoint("hdfs://hadoop01:9000/stream/chk")
    val stream = ssc.textFileStream("hdfs://hadoop01:9000/stream")

    val result1 = stream.flatMap{line => line.split(" ")}.map{(_, 1)}
    //利用检查点机制保持更新
    val result2 = result1.updateStateByKey{(seq, op:Option[Int])=> Some(seq.sum + op.getOrElse(0))}

    result2.print()
    ssc.start()
    //保持一直开启SparkStreaming
    ssc.awaitTermination()
  }
}
