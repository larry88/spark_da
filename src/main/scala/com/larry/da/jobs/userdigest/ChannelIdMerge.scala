package com.larry.da.jobs.userdigest

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkContext

/*

import com.larry.da.jobs.userdigest.UserMapping;
import com.larry.da.jobs.userdigest.Config

*/

/**
  * Created by larry on 14/12/15.
  */
object ChannelIdMerge {
  var sc : SparkContext = _



  def mergeIdMap(sc:SparkContext,day:String): Unit ={
    this.sc = sc
    // val day = "2016-01-17"
    val sdf = new SimpleDateFormat("yyyy-MM-dd");
    val lastDay = new Date( sdf.parse(day).getTime - 60*60*24*1000 )
    val historyDay = sdf.format(lastDay)
    mergeIdMap(historyDay,day)
  }




  def mergeIdMap(historyDay:String, day:String): Unit = {
    val uidChange = aguidChange(day)
    uidChange.map(x=>{ val(u1,u2) = x; Array(u1,u2).mkString("\t") }).saveAsTextFile(s"${Config.historyIdMapUidChangePath}/$day")
//    uidChange.map(x=>{ val(u1,u2) = x; Array(u1,u2).mkString("\t") }).saveAsTextFile(s"aguid/idmapHistory/uidChange/$day")

    val uidDic = sc.broadcast(uidChange.collectAsMap())

    def aguid4Channel(historyDay: String, day: String): Unit = {
      val timeLimitDown = Config.timeLimitDown(day)
      val historyChannel = sc.textFile(s"${Config.historyIdMapChannelPath}/$historyDay").map(UserMapping(_)).filter(_.time >= timeLimitDown).map(u => (u.cid, u))
      val channelToday = sc.textFile(s"${Config.dayIdMapPath}/$day*/channelid").map(UserMapping(_)).map(u => (u.cid, u))
      val mergeData = historyChannel.union(channelToday).reduceByKey((a,b)=>a.merge(b),Config.partitionCount).map(_._2)
      mergeData.map(p => {p.uid = uidDic.value.getOrElse(p.uid, p.uid);p}).saveAsTextFile(s"${Config.historyIdMapChannelPath}/$day", classOf[GzipCodec])
//      mergeData.map(p => {p.uid = uidDic.value.getOrElse(p.uid, p.uid);p}).saveAsTextFile(s"aguid/idmapHistory/channel/$day", classOf[GzipCodec])
    }

    def aguid4Agsid(day: String): Unit = {
      val agsidToday = sc.textFile(s"${Config.dayIdMapPath}/$day*/agsid").map(UserMapping(_)).map(u => ((u.cid, u.idType), u)).reduceByKey((a, b) => a.merge(b), 150).map(_._2)
      agsidToday.map(p => {p.uid = uidDic.value.getOrElse(p.uid, p.uid);p}).saveAsTextFile(s"${Config.historyIdMapAgsidPath}/$day", classOf[GzipCodec])
//      agsidToday.map(p => {p.uid = uidDic.value.getOrElse(p.uid, p.uid);p}).saveAsTextFile(s"aguid/idmapHistory/agsid/$day", classOf[GzipCodec])
    }

    //------channelid-----------
    aguid4Channel(historyDay, day);
    //------agsid-----------
    aguid4Agsid(day)

  }



  def aguidChange(day:String) = {
    val rddList =  "07,15,23".split(",").map(hour=>sc.textFile(s"/user/dauser/aguid/hbase/${day}-$hour/verticesDel").map(_ + "\t" + hour)).map(rdd=>{
      rdd.map(x=>{
        val Array(u1,u2,hour) = x.split("\t")
        (u1,(u2,hour))
      })
    })
    val log = sc.union( rddList )
    val data = log.reduceByKey((a,b)=>if(a._2 > b._2) a else b,20)
    data.map(x=>{
      val (u1,(u2,hour)) = x
      (u1,u2)
    })
  }


}
