package com.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sql")
    val sc = new SparkContext(conf)
    val sqc = new SQLContext(sc);

    val r1 = sc.makeRDD(List((1,"tom"),(2,"rose"),(3,"jary"),(4,"wangwenjie")))
    val t1 = sqc.createDataFrame(r1).toDF("id","name")

    t1.registerTempTable("tab2")

    val result = sqc.sql("select * from tab2 where name = 'tom'")
    result.toJavaRDD.saveAsTextFile("hdfs://hadoop01:9000/sql/testTom.txt")

  }
}
