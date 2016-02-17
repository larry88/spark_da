package com.larry.da.jobs.userdigest

import org.apache.spark.{HashPartitioner, SparkContext}
import scala.collection.mutable.ArrayBuffer
import com.google.common.hash.Hashing.md5

/**
  * Created by larry on 13/1/16.
  */
object RealTimeTag {
  var sc: SparkContext = _

  def tt(): Unit ={

    val log = sc.textFile("/user/tracking/userdigest/log/hourly/*realtime_tags*2016-01-12*gz").map(x=>{
      val Array(sendType,channid,agsid,tag,time) = x.split("\t")
        val chn = sendType match {
          case "0" => 2
          case "1" => 3
          case "3" => 4
          case _ => 1
        }
      val dspid = if(chn == 3 && channid.length > 32) md5.hashString(channid,com.larry.da.jobs.idmap.Config.chaset_utf8).toString else channid
        (chn,dspid,agsid,tag)
    }).filter(x=>x._2 != "" || x._3 != "")

    val line = sc.textFile("/tmp/mapred/userdigest/rttag/2016-01-12").filter(_.contains("FU_2@160112"))

    val chnDic = sc.textFile("/user/dauser/aguid/idmapHistory/channel/2016-01-12").map(x=>{ val Array(cid,uid,idType) = x.split("\t").take(3); ((idType.toInt,cid),uid) })
    val agsidDic = sc.textFile("/user/dauser/aguid/idmapHistory/agsid/2016-01-12").map(x=>{ val Array(cid,uid) = x.split("\t").take(2); (cid,uid) })

    val agsidMapped = log.filter(_._3 != "").map(x=>{
      val (chn,channid,agsid,tag) = x;
      (agsid,(chn,channid,tag))
    }).leftOuterJoin(agsidDic).map(x=>{
      val (agsid,((chn,channid,tag),aguid)) =x
      val uid = aguid match {case Some(u:String)=> u; case _ => ""}
      ((chn,channid),(uid,agsid,tag))
    })

    val chlData = log.filter(_._3 == "").map(x=>{
      val (chn,channid,agsid,tag) = x;
      ((chn,channid),("",agsid,tag))
    }).union(
      agsidMapped.filter(_._2._1 == "")
    )

    val chlMapped = chlData.join(chnDic).map(x=>{
      val  ((chn,channid),((xx,agsid,tag),uid))= x;
      (uid,tag)
    })

    val res = agsidMapped.map(x=>{
      val ((chn,channid),(uid,agsid,tag)) = x;
      (uid,tag)
    }).union(chlMapped).aggregateByKey(new ArrayBuffer[String]())(
      (a,b) => a += b,
      (a,b) => a ++= b
    ).mapValues(_.mkString("|~|"))

    res.count


  }

}
