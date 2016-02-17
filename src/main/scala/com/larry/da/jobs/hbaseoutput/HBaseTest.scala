package com.larry.da.jobs.hbaseoutput

/**
  * Created by larry on 16/12/15.
  */


import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Scan, HBaseAdmin}
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark._


object HBaseTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HBaseTest")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"))
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))

    val hbaseContext = new HBaseContext(sc, conf)

    val scan = new Scan()
    scan.setCaching(100)

    val getRdd = hbaseContext.hbaseRDD("userdigestuid", scan)
    getRdd.take(100).foreach(println)

    sc.stop()
  }
}

