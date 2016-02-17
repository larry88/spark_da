package com.larry.da.test

/**
  * Created by larry on 17/2/16.
  */


import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext


object HBaseBulkPutExample {
  val sc:SparkContext = _

  def main(args: Array[String]) {
//    if (args.length == 0) {
//      System.out.println("HBaseBulkPutExample {tableName} {columnFamily}");
//      return;
//    }
//    val tableName = args(0);
//    val columnFamily = args(1);


    val tableName = "userdigestuid_test";
    val columnFamily = "cf1";

    /********************************************************
    * hbase行要求:
    * (key,Array((cf,fieldname,value),(cf,fieldname,value)))
    * ******************************************************/
    val rdd = sc.parallelize(Array(
      (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("1")),(Bytes.toBytes(columnFamily), Bytes.toBytes("11"), Bytes.toBytes("11")))),
      (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("2")))),
      (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("3"))))
    ))

    val conf = HBaseConfiguration.create();
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
    val hbaseContext = new HBaseContext(sc, conf);
    hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd,
      tableName,
      (putRecord) => {
        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => put.add(putValue._1, putValue._2, putValue._3))
        put
      },
      true);

  }
}
