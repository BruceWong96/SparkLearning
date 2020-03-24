package com.kryo

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("kryoTest")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator","com.kryo.MyKryoRegister")

    val sc = new SparkContext(conf)
    val r1 = sc.makeRDD(List(new Person("tom", 21)
              ,new Person("rose", 23)))
    //以序列化的形式缓存
    r1.persist(StorageLevel.MEMORY_ONLY_SER)

  }

}
