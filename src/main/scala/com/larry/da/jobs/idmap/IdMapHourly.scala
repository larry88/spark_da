package com.larry.da.jobs.idmap

/**
  * Created by larry on 19/10/15.
  */

/************************************************
  * id type is :
  *   case "aguid" => "0"
  *   case "agsid" => "1"
  *   case "adx" =>   "2"
  *   case "baidu" => "3"
  *   case "tanx" =>  "4"
  *   case "agfid" => "5"
  * *********************************************/

//import com.larry.da.jobs.idmap.Config
import java.text.SimpleDateFormat

import com.google.common.hash.Hashing.md5
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ArrayBuffer, HashMap}


object IdMapHourly {
  var sc:SparkContext = _

  var parallelism = 60 // sc.defaultParallelism
  var grapParallelism = 20 // sc.defaultParallelism
  val hourlyPath = "/user/dauser/aguid/hourly"

  //========== vertices and edges =================
  def prepareGragh(data: RDD[((String,String,String, String), String)]) = {
    val prepare = data.reduceByKey( (a,b)=>if(a>=b) a else b,grapParallelism )
    val res = prepare.flatMap(x => {
      val ((id1,id1Type, id2, id2Type), time) = x;
      val Array(id1L, id2L) = Array(id1,id2).map(md5.hashString(_,Config.chaset_utf8).asLong())
      val list = new ArrayBuffer[(Int,(Long,String,String,String))]()
      if(id1 != "") list +=( (1,(id1L,id1,id1Type,time)) )
      if(id2 != "") list +=( (1,(id2L,id2,id2Type,time)) )
      if(id1 != "" && id2 != "") list +=( (2,(id1L,id2L.toString,"","")) )
      list
    }).cache()

    val vertices = res.filter(_._1 == 1).map(x => {
      val (xx,(guidL, guid, idType, time)) = x;
      (guidL, (guid, idType, time,1))
    }).reduceByKey((a, b) => {
      val res = if (a._3 > b._3) a else b;
      (res._1,res._2,res._3,a._4 + b._4)
    })

    val edges = res.filter(_._1 == 2).map(x => {
      val (xx,(guidL, agsidL,xx3,xx4)) = x;
      Edge(guidL,agsidL.toLong,null)
    })

    res.unpersist()
    (vertices, edges)
  }



  /** **********************************************
    * compute hour's data
    ***********************************************/
  def computeHourly(rdd:RDD[((String,String,String,String),String)]) = {
    val (vertices, edges) = prepareGragh(rdd)
    val data = Graph.fromEdges(edges.coalesce(grapParallelism, true),null).connectedComponents().vertices
    //rightOuterJoin,if find => uid=uidL else uid=guidL
    val hourAllIndex = data.rightOuterJoin(vertices, grapParallelism).map(x => {
      val (guidL, (uidL, (guid, idType, time, num))) = x;
      val uid = uidL match { case Some(id:Long) => id; case _ => guidL }
      (guidL, (uid, guid, idType, time, num))
    })
    hourAllIndex.mapPartitions(p => {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      p.map(x => {
        val (guidL, (uidL, guid, idType, time, num)) = x;
        val minites = try { (sdf.parse(time).getTime / 60000).toInt } catch { case e: Exception => 0 }
        (guidL, uidL, guid, idType, minites, num)
      })
    }).filter(_._5 != 0).map(x => {
      val (guidL, uidL, guid, idType, minites, num) = x;
      Array(guidL, uidL, guid, idType, minites, num).mkString("\t")
    })
  }


  def getFileHour(src:Iterable[String]) ={
    val dic = new HashMap[String,ArrayBuffer[String]]
    src.foreach(p=>{
      val hour10 = p.take(12)
      if(dic.contains(hour10))
        dic(hour10) += p(12).toString
      else
        dic.put(hour10,ArrayBuffer(p(12).toString))
    })
    val res = new ArrayBuffer[String]
    dic.foreach(p=>{
      val (k,v) = p
      res += k + "[" + v.head + "-" + v.last + "]"
    })
    res
  }

  def computeHourList(sparkContex:SparkContext,para:Int,hourList:ArrayBuffer[String])={
    sc = sparkContex
    parallelism = para
    grapParallelism = para / 3

    val fileHours = getFileHour(hourList)
    val rdd = sc.union(
      Config.getDataSource().flatMap(d=> fileHours.map(h => d.replace("{YYYY-MM-DD-HH}",h)))
        .map(s=>
          sc.textFile(s).map(x=>{
            val Array(time,id1,id1Type,id2,id2Type) = x.split("\t",-1).map(d=>{ val v = d.trim; if (v == "null") "" else v })
            ((id1,id1Type,id2,id2Type),time)
          })
        )
    ).repartition(parallelism * 3)

    hourList.foreach(hour => {
      val workHour = hour.substring(0,10) + " " + hour.substring(11)
      val dataRdd = rdd.filter(_._2.startsWith(workHour))
      val saveHour = hour.replace(" ","-").take(13)
      computeHourly(dataRdd).saveAsTextFile(s"${hourlyPath}/${saveHour}",classOf[GzipCodec])
    })
  }




  //  //========== cm,adshow,tt =================
  //  def getDspRdd(day:String)={
  //    import com.larry.da.util.{LogParseUtil => U}
  //    val fields = "time,channel,guid,agsid".split(",")
  //    val log = sc.union(
  //      day.split(",").flatMap(hour=>{
  //        "cm,show,tt".split(",").flatMap(logType => {
  //          "baidu,adx,tanx".split(",").map(ch=> U.dspRdd(sc,logType,ch,hour).map(x=>x + "|~|channel=" + ch ).map(x=>{
  //            if(logType == "cm") U.cmLog(x)
  //            else if(logType == "show") U.showLog(x)
  //            else U.ttLog(x)
  //          }) )
  //        })
  //      })
  //    )
  //    val data = log.map(d=>{
  //      val Array(time,channel,guid,agsid) = fields.map(f => {val v = d.getOrElse(f,"null"); if (v== "") "null" else v})
  //      val idType = channel match {
  //        case "adx" => "2"
  //        case "baidu" => "3"
  //        case "tanx" => "4"
  //      }
  //      val logtime = time.drop(2).replace(" ","").replace(":","").replace("-","").take(10)
  //      val guidRes = if(guid == "MOCKEDGUIID") "null" else if(idType == "3" && guid.length > 32) md5.hashString(guid,Config.chaset_utf8).toString else guid
  //      ((guidRes,agsid,idType),logtime)
  //    })
  //    data.filter(x=>x._1._1 != "null" && x._1._2 != "null")
  //
  //  }
  //
  //
  //  //========== pv and tracking =================
  //  def getPvRdd(day:String)={
  //    val dic = sc.broadcast( sc.textFile("/user/tracking/log/comm/cid_mapping").map(_.split("\t")).map(x=>(x(0),x(1))).collectAsMap() )
  //    import com.larry.da.util.{LogParseUtil => U}
  //    val log = sc.union(
  //      day.split(",").flatMap(hour=>{
  //        "pv,tracking".split(",").map(ssp =>{
  //          U.pvRdd(sc,ssp,hour)
  //        })
  //      })
  //    )
  //    val pvFields = "time,agsid,query".split(",")
  //    val data = log.map(x=>{
  //      val d = U.pvLog(x)
  //      val Array(time,agsid,query) = pvFields.map(f => {val v = d.getOrElse(f,"null"); if (v== "") "null" else v})
  //      val agfid =  query.split("agfid=",2) match {
  //        case Array(f1,f2) => f2.split("\\W").head.replace("\t","");
  //        case _ => "null"
  //      }
  //      val atscu =  query.split("atscu=",2) match {
  //        case Array(f1,f2) => f2.split("\\W").head.replace("\t","");
  //        case _ => "null"
  //      }
  //      val cid = dic.value.getOrElse(atscu,"null")
  //      val fid = if(cid == "null" || agfid == "null") "null" else cid + "_" + agfid
  //      val logtime = time.drop(2).replace(" ","").replace(":","").replace("-","").take(10)
  //
  //      //pre tracking,only agfid, no agsid
  //      val fieldCount = x.split("\t|\\|~\\|",-1).length
  //
  //      //not pre tracking,if fid == "null" => set agsid = "null"
  //      val sid = if(fieldCount != 16 && fid == "null") "null" else agsid
  //      ((fid,sid.replace("\t",""),"5"),logtime)
  //    })
  //    //be sure: agsid != null
  //    data.filter(_._1._2 != "null")
  //  }
  //



  //
  //  def computeDay(day:String): Unit ={
  //    val (cm, pv) = (getDspRdd(day), getPvRdd(day))
  //    val dayRdd = sc.union(cm,pv).coalesce(parallelism * 3)
  //
  //    0 to 23 map(h=>{
  //      val hour = Array(day, { if(h>9) h.toString else "0"+h }).mkString("-")
  //      val workHour = hour.drop(2).replace(" ","").replace(":","").replace("-","").take(10)
  //      computeAndSave(dayRdd.filter(_._2.take(8) == workHour),hour)
  //    })
  //  }
  //
  //
  //  def main(args: Array[String]): Unit = {
  //    val sparkConf = new SparkConf().setAppName("IdMap-hourly")
  //    sc = new SparkContext(sparkConf)
  ////   val args = ("2015-10-26-08,15,60").split(",")
  //    1 to args.length zip args foreach(println)
  //    val Array(baseHour,runCount,partitionCount) = args
  //    parallelism = partitionCount.toInt
  //    grapParallelism = parallelism / 3
  //    val sdf=new SimpleDateFormat("yyyy-MM-dd-HH");
  //    val startHour= Calendar.getInstance()
  //    startHour.setTime( sdf.parse(baseHour) );
  //    val hourList = new ArrayBuffer[String]
  //    0 until runCount.toInt foreach(n=> {
  //      val day = sdf.format(startHour.getTime)
  //      println(day)
  //      hourList += day;
  //      startHour.add(Calendar.HOUR, 1);
  //    })
  //  }


}
