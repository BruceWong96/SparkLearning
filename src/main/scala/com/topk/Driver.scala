package com.topk

import org.apache.spark.{SparkConf, SparkContext}

object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TopK")
    val sc = new SparkContext(conf)
    val data = sc.textFile("hdfs://hadoop01:9000/topk/topk.txt",2)

    val result = data.flatMap{line=>line.split(" ")}
      .map{word=>(word, 1)}.reduceByKey(_+_)
      .sortBy{case(word,count) => -count}
      .take(3)

    result.foreach(println)
  }

}
