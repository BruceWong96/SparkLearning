package com.maxmin

import org.apache.spark.{SparkConf, SparkContext}
//输出man的最大值
object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("MaxMin")
    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://hadoop01:9000/maxmin/maxmin.txt",2)

    //先分割，再过滤
    val result = data.map(line => line.split(" "))
                     .filter(arr => arr(1).equals("M"))
      .map(arr => arr(2).toInt).max()
    println(result)

  }

}
