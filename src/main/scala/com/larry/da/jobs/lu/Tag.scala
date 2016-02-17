package com.larry.da.jobs.lu

import org.apache.spark.SparkContext

import scala.collection.mutable

/**
  * Created by larry on 29/12/15.
  */
object Tag {
  var sc:SparkContext = _

  def tt(): Unit ={

    val day="151228"

    val keys1 = "投资,赚钱,理财,期货,大宗商品".split(",")
    val keys2 = "石油,原油".split(",")
    val log = sc.textFile("/user/dsp/bidder_agg/baidulu/agg/2015-12-29-08/data/lukwgz/").filter(x=>{
      keys1.filter(x.contains(_)).size > 0
    })

    val data = log.map(x=>x.split("\t",2)).map(x=>(x(0),x(1))).map(x=>{
      val(k,v)=x;
      var tag = "NDI_1800_3"
      if(keys2.filter(v.contains(_)).length > 0){
        tag = "NDI_1800_2"
      }
      (k,tag)
    }).aggregateByKey(new mutable.HashSet[String])(
      (a,v) => a += v,
      (a,b) => a ++= b
    ).map(x=>{val(k,v)=x;(k,v.toArray.map(_ + "@$day").mkString(";"))})

    data.take(100).foreach(println)

  }

}
