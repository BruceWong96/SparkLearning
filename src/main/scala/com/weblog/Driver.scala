package com.weblog


import org.apache.hadoop.fs.shell.find.Result
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Driver {
  val conf = new SparkConf().setMaster("local[2]").setAppName("weblog")
  val sc = new SparkContext(conf)

  //  保存数据到hbase
  def saveToHbase(m: Map[String, String], rowKey:String)={
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort","2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "weblog")

    //    获取操作hbase的job对象
    val job = new Job(sc.hadoopConfiguration)

    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    //org.apache.hadoop.fs.shell.find.Result
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])



    val r=sc.makeRDD(Array("line"))
    val hbaseRDD=r.map { x =>{
      val url=m.apply("url")
      val urlname=m.apply("urlname")
      val uvid=m.apply("uvid")
      val ssid=m.apply("ssid")
      val sscount=m.apply("sscount")
      val sstime=m.apply("sstime")
      val cip=m.apply("cip")


      //--产出行对象，并指定行键
      val put=new Put(Bytes.toBytes(rowKey))
      put.add(Bytes.toBytes("cf1"),Bytes.toBytes("url"),Bytes.toBytes(url))
      put.add(Bytes.toBytes("cf1"),Bytes.toBytes("urlname"),Bytes.toBytes(urlname))
      put.add(Bytes.toBytes("cf1"),Bytes.toBytes("uvid"),Bytes.toBytes(uvid))
      put.add(Bytes.toBytes("cf1"),Bytes.toBytes("ssid"),Bytes.toBytes(ssid))
      put.add(Bytes.toBytes("cf1"),Bytes.toBytes("sscount"),Bytes.toBytes(sscount))
      put.add(Bytes.toBytes("cf1"),Bytes.toBytes("sstime"),Bytes.toBytes(sstime))
      put.add(Bytes.toBytes("cf1"),Bytes.toBytes("cip"),Bytes.toBytes(cip))

      (new ImmutableBytesWritable,put)
    }

  def main(args: Array[String]): Unit = {

    val ssc = new StreamingContext(sc, Seconds(5))
    val zkHosts = "hadoop01:2181,hadoop02:2181,hadoop03:2181"
    val group = "group2"
    val topics = Map("weblog"->1)
    val stream = KafkaUtils.createStream(ssc, zkHosts, group, topics)
      .map(_._2)

    //(url,urlname,uvid,ssid,sscount,sstime,cip)
    val d1 = stream.map{line =>
      val info = line.split("\\|")
      val url = info(0)
      val urlName = info(1)
      val uvid = info(13)
      val ssid = info(14).split("_")(0)
      val sscount = info(14).split("_")(1)
      val sstime = info(14).split("_")(2)
      val cip = info(15)

      val map = Map("url" -> url,
        "urlName"->urlName,
        "uvid"->uvid,
        "ssid"->ssid,
        "sscount"->sscount,
        "sstime"->sstime
      )
      val rowKey = sstime + "_" + uvid + "_" + ssid + "_" + (Math.random()*100).toInt
      saveToHbase(map, rowKey)

    }
//    d1.print()

//    stream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
