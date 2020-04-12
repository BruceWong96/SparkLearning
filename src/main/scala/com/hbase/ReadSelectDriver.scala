package com.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.RandomRowFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.{SparkConf, SparkContext}

object ReadSelectDriver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("read")
    val sc = new SparkContext(conf)

    val hbaseConf = HBaseConfiguration.create()

    hbaseConf.set("hbase.zookeeper.quorum","hadoop01,hadoop02,hadoop03")
    hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "tabx")

    val scan = new Scan

    val filter = new RandomRowFilter(0.5f)
    scan.setFilter(filter)
    hbaseConf.set(TableInputFormat.SCAN,
      Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))

    val resultRDD =  sc.newAPIHadoopRDD(hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      //      org.apache.hadoop.hbase.client.Result
      classOf[Result]
    )

    resultRDD.foreach{x =>
      val rowResult = x._2
      val rowKey = Bytes.toString(rowResult.getRow)
      val name = Bytes.toString(rowResult.getValue("cf1".getBytes,"name".getBytes))
      val age = Bytes.toString(rowResult.getValue("cf1".getBytes,"age".getBytes))
      println(rowKey+": "+ name + ": " + age)
    }

  }
}
