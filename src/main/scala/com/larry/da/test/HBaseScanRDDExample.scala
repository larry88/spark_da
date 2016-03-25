package com.larry.da.test

/**
  * Created by larry on 1/3/16.
  */

import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Scan
import java.util.ArrayList
import org.apache.spark.SparkConf
import com.cloudera.spark.hbase.HBaseContext


object HBaseScanRDDExample {
  var sc:SparkContext = _

  def main(args: Array[String]) {
//    if (args.length == 0) {
//      System.out.println("GenerateGraphs {tableName}")
//      return ;
//    }
//    val tableName = args(0);

    sc.addJar("/opt/cloudera/parcels/CDH-5.4.0-1.cdh5.4.0.p0.27/jars/htrace-core-3.1.0-incubating.jar");

    val tableName = "userdigestuid"

    val conf = HBaseConfiguration.create()
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"))
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))

    var scan = new Scan()
//    scan.addFamily(Bytes.toBytes("cftag"));
//    scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("searchwords"));
    scan.setCaching(100)

    val hbaseContext = new HBaseContext(sc, conf);

    var getRdd = hbaseContext.hbaseScanRDD( tableName, scan)

    println(" --- abc")
    getRdd.foreach(v => println(Bytes.toString(v._1)))
    println(" --- def")
    getRdd.collect.foreach(v => println(Bytes.toString(v._1)))
    println(" --- qwe")

  }
}
