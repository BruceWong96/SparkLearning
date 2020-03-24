package com.median

import org.apache.spark.{SparkConf, SparkContext}

object Driver {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("median")
    val sc = new SparkContext(conf)

    val data = sc.textFile("h://test/median.txt",2)

    val result = data.flatMap{ line => line.split(" ")}
                     .map{ num => num.toInt}.sortBy{num => num}

    val medianIndex = (result.count() + 1) / 2

    val median = result.take(medianIndex.toInt).last

    println(median)

  }

}
