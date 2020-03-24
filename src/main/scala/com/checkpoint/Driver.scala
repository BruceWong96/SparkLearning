package com.checkpoint

import org.apache.spark.{SparkConf, SparkContext}

object Driver {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("chk")
    val sc = new SparkContext(conf)

    //指定检查点的存储目录
    sc.setCheckpointDir("h://test/chk")
    val r1 = sc.makeRDD(List(1,2,3,4,5))

    //可以在整个DAG中,选择一些比较重要的RDD,做cache和checkpoint
    //底层处理方式: 优先去cache中去恢复,如果cache丢失,则去check目录恢复
    r1.cache()
    r1.checkpoint()
  }

}
