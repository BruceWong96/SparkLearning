package com.average

import org.apache.spark.{SparkConf, SparkContext}

object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Average")
    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://hadoop01:9000/average/average.txt",2)

    val tmp = data.map{line => line.split(" ")}.map(arr=>arr(1).toInt)

    val sum = tmp.sum()
    val count = tmp.count()

    if (count != 0){
      val result = sum/count
//      val resultFile = sc.makeRDD(List(result)).saveAsTextFile("hdfs://hadoop01:9000/average/result")
      println(result)
    }
  }

}
