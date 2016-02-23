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

//import com.agrantsem.dm.jobs.idmap.Config
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
          sc.textFile(s).map(_.split("\t",-1)).filter(_.size >= 5).map(_.take(5)).map(x=>{
            val Array(time,id1,id1Type,id2,id2Type) = x.map(d=>{ val v = d.trim; if (v == "null") "" else v })
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




}
