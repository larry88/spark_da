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
import org.apache.spark.SparkConf
import com.cloudera.spark.hbase.HBaseContext

object HBaseBulkGetExample {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.out.println("HBaseBulkGetExample {tableName}");
      return ;
    }

    val tableName = args(0);

    val sparkConf = new SparkConf().setAppName("HBaseBulkGetExample " + tableName)
    val sc = new SparkContext(sparkConf)


    //[(Array[Byte])]
    val rdd = sc.parallelize(Array(
      (Bytes.toBytes("1")),
      (Bytes.toBytes("2")),
      (Bytes.toBytes("3")),
      (Bytes.toBytes("4")),
      (Bytes.toBytes("5")),
      (Bytes.toBytes("6")),
      (Bytes.toBytes("7"))))

    val conf = HBaseConfiguration.create()
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"))
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))

    val hbaseContext = new HBaseContext(sc, conf);

    val getRdd = hbaseContext.bulkGet[Array[Byte], String](
      tableName,
      2,
      rdd,
      record => {
        System.out.println("making Get" )
        new Get(record)
      },
      (result: Result) => {

        val it = result.list().iterator()
        val b = new StringBuilder

        b.append(Bytes.toString(result.getRow()) + ":")

        while (it.hasNext()) {
          val kv = it.next()
          val q = Bytes.toString(kv.getQualifier())
          if (q.equals("counter")) {
            b.append("(" + Bytes.toString(kv.getQualifier()) + "," + Bytes.toLong(kv.getValue()) + ")")
          } else {
            b.append("(" + Bytes.toString(kv.getQualifier()) + "," + Bytes.toString(kv.getValue()) + ")")
          }
        }
        b.toString
      })


    getRdd.collect.foreach(v => System.out.println(v))

  }
}
