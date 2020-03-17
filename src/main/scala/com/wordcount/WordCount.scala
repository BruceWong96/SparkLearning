package com.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //创建Spark的环境参数对象，并指定运行模式（集群模式），以及Job名称（自定义）
    val conf = new SparkConf().setMaster("spark://hadoop01:7077")
      .setAppName("WordCount")

    //获取Spark的上下文对象，通过此对象操作Spark
    val sc = new SparkContext(conf)

    //读取数据源（这里是从hdfs里读取），分区设置为2
    val data = sc.textFile("hdfs://hadoop01:9000/word/words.txt", 2)

    val result = data.flatMap{_.split(" ")}.map{(_, 1)}.reduceByKey(_+_)

    //    modify the number of partitions to one
    result.coalesce(1,true).saveAsTextFile("hdfs://hadoop01:9000/word/result")
  }

}
