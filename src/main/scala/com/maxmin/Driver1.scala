package com.maxmin

import org.apache.spark.{SparkConf, SparkContext}

object Driver1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("MaxLine")
    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://hadoop01:9000/maxmin/maxmin.txt")

    val result = data.map(line => line.split(" "))
      .filter(arr => arr(1).equals("M"))
      .sortBy(arr => -arr(2).toInt).take(1)
      .map(arr => (arr(0), arr(1), arr(2)))

    result.foreach(arr => println(arr._1 + " " + arr._2 + " " + arr._3))
  }

}
