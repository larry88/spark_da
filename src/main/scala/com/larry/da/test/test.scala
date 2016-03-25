package com.larry.da.test

import org.apache.spark.SparkContext

import com.larry.da.util.{LogParseUtil => U}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

/**
  * Created by larry on 19/2/16.
  */
object test {
  import scala.reflect.runtime.universe._

  def tt(): Unit ={

    var url="http://www.baidu.com"
    val host = url.replaceAll("(^http(s)?:\\/\\/)?", "").replaceAll("\\/.*$|\\?.*$|:.*$", "").toLowerCase();
    val regex = """([\w-]+(\.(com|net|org|gov|me|name|edu|info|tel|mobi|tv|cc|biz|asia|so|co))?(\.(cn|hk|us|sg|jp|in|pw|am|ph|tj))?)$""".r
    val m = regex.findFirstIn(host)

  }

  def getUvPv(sc: SparkContext, channel: String, time: String, dtype: String): Unit = {
//    import com.agrantsem.dm.util.{ LogParseUtil => U }

    val rdd = U.dspRdd(sc, "bidder", channel, time)
    val bidderFields = "url,agUserId,ads".split(",")

    val bidder = rdd.map(x => {
      val d = U.bidderLog(x);
      val Array(url, uid, ads) = bidderFields.map(k => { val v = d.getOrElse(k, ""); if (v == "null") "" else v })
      val domain = "" //if (dtype.equals("first")) U.getFirtDomainFromUrl(url) else U.getSecondDominFromUrl(url)
      ((domain,uid), (if (ads.contains("mid")) 1 else 0, 1))
    }).reduceByKey((a,b)=>(a._1 + b._1,a._2 + b._2))


    val res = bidder.map(x=>{
      val((domain,uid),(pvmid,pv)) = x;
      (domain,(pvmid,if(pvmid>0)1 else 0,pv,1))
    }).reduceByKey((a,b)=>(a._1+b._1,a._2+b._2,a._3+b._3,a._4 + b._4))

  }

}

object testHashMap extends App{

  import com.larry.da.util.Timed
  import scala.collection.mutable.HashMap

  val hashMap = HashMap(1.toLong -> 1.toLong)

  0 to 300 foreach(i => println( Timed.timed{ 0 to 200000 foreach(x=>hashMap += (x*i.toLong -> x*i.toLong) ); }) )

}
