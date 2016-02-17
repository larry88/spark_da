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

import java.text.SimpleDateFormat
import java.util.Calendar

import com.google.common.hash.Hashing.md5
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/*

import com.larry.da.jobs.idmap.Person
import com.larry.da.jobs.idmap.Config
import com.larry.da.jobs.idmap.Utils

*/


object IdMapHistory_test {
  var sc:SparkContext = _

  var parallelism = 60 // sc.defaultParallelism
  var graphParallelism = 20 // sc.defaultParallelism
  val partitionCount = Config.partitionCount
  val historyPath = "/user/dauser/aguid/history"
  val hbasePath = "aguid/hbase"
  val idmapPath = "aguid/idmap"
  val hourlyPath = "aguid/hourly"
  val hbaseOutPath = "/user/dauser/aguid/hbase_output"

  val verticesLimit = 50
  val logtimeFormat=new SimpleDateFormat("yyyy-MM-dd-HH");

  var mergeData:RDD[Person] = _
  var uidChange : RDD[(Long,(Long,Int))] = _
  var hourRdds : Array[RDD[Person]] = _


  def reduceHourData(data: RDD[Person]) = {
    data.map(p=>{
      ((p.uid,p.idType,p.time/10),p)
    }).aggregateByKey( new ArrayBuffer[Person])(
      (arr,v) => arr += v,
      (arr1,arr2) => arr1 ++= arr2
    ).map(x=>{
      val ((uid, idType,m10), list) =x
      ((uid,idType),(m10,list.sortWith((a,b)=>if(a.time != b.time) a.time > b.time else a.cid > b.cid).take(verticesLimit)))
    }).aggregateByKey(new ArrayBuffer[(Int,ArrayBuffer[Person])])(
      (arr,v) => arr += v,
      (arr1,arr2) => arr1 ++= arr2
    ).flatMap(x=>{
      val ((uid,idType),list) =x;
      val res = list.sortWith((a,b)=>a._1 > b._1).map(_._2)
      res.tail.foldLeft(res.head)((a,b)=>if(a.length < verticesLimit) a ++= b else a).take(verticesLimit)
    })
  }


  def getIndexHis(day:String,lastDay:String)={
    val hours = day.split(",")
    if(lastDay.split("-").last == "23"){
      val day = hours.head.take(10)
      val path = s"$hbaseOutPath/$day" //***
      //      val path = "/user/dauser/aguid/hbase_output/2016-01-09/part-m-0000[0-5]"
      sc.textFile(path).map(_.split("\t")).map(x=>{
        val Array(uid,cid,idType,time,num) =x;
        new Person(md5.hashString(cid,Config.chaset_utf8).asLong(),Utils.unCompressAguid(uid),cid,idType.toInt,time.toInt,num.toInt)
      }).map(p=>(p.cidL,p))
    } else {
      val path = s"$historyPath/$lastDay"
      //      sc.textFile(path).map(_.split("\t")).map(x => { val Array(guidL,uidL,guid,idType,time,num) =x; (guidL.toLong, (uidL.toLong,guid,idType,time.toInt,num.toInt)) })
      sc.textFile(path).map(Person(_)).map(p=>(p.cidL,p))
    }
  }


  def hbaseAddFormat(rdd:RDD[Person])={
    rdd.map(p => {
      ((p.uid, p.idType), p)
    }).aggregateByKey(new ArrayBuffer[Person], parallelism)(
      (arr, v) => arr += v,
      (arr1, arr2) => arr1 ++= arr2
    ).map(x => {
      val ((uid, idType), list) = x;
      (uid, (idType, list.sortWith((a, b) => a.time > b.time).map(p => Array(p.cid, p.time, p.num).mkString("|")).mkString(","), list.length))
    }).aggregateByKey(new ArrayBuffer[(Int, String, Int)])(
      (arr, v) => arr += v,
      (arr1, arr2) => arr1 ++= arr2
    ).map(x => {
      val (uid, list) = x;
      val idsText = ArrayBuffer("", "", "", "", "")
      var vertexCount = 0;
      list.foreach(p => {
        val (ix, ids, vc) = p;
        idsText(ix.toInt - 1) = ids;
        vertexCount += vc
      })
      val uidS = Utils.compressAguid(uid)
      Array(uidS, idsText.mkString("\t"), vertexCount).mkString("\t")
    })
  }

  def setOldestUid(rdd:RDD[(Long,(Long,Int))])= {
    rdd.map(x => {
      val (u1, (u2, time)) = x
      (u2, (u1, time))
    }).aggregateByKey(new ArrayBuffer[(Long, Int)])(
      (arr, v) => arr += v,
      (arr1, arr2) => arr1 ++= arr2
    ).flatMap(x => {
      val (uid, list) = x;
      var oldest = list.head
      list.foreach(p => if (p._2 < oldest._2) oldest = p else if (p._2 == oldest._2 && p._1 < oldest._1) oldest = p)
      val v2 = oldest._1
      list.map(p => {
        val (v1, time) = p;
        (v1, (v2, time))
      })
    })
  }

  /** **********************************************
    * compute hour's data
    ***********************************************/
  def compute(hourList:String, historyHour:String): Unit = {

    val hours = hourList.split(",")
    val (hourSave,limitDown) = (hours.last, ( logtimeFormat.parse( hours.head ).getTime /60000 ).toInt )

    hourRdds = hours.map(hour => sc.textFile(s"${hourlyPath}/$hour").map(Person(_)))
    val hourAllIndex = sc.union(hourRdds).repartition(parallelism * 3)

    //reduce hour count to verticesLimit
    val indexHourReduce =  reduceHourData(hourAllIndex).map(p=>(p.cidL,p))

    //from history index file
    val indexHis = getIndexHis(hourList, historyHour)
    mergeData = indexHis.union(indexHourReduce).reduceByKey((a, b) => a.merge(b, true), Config.partitionCount).map(_._2) //***

    val uidList = mergeData.filter(p => p.uidSet != null && p.uidSet.size > 1).map(_.uidSet.toArray).repartition(parallelism)
    val vertices = uidList.flatMap(x => x).reduceByKey((a, b) => if (a < b) a else b)
    val edges = uidList.flatMap(li => { val v1 = li.head._1; li.tail.map(v2 => new Edge(v1, v2._1, null)); })

    val uidChangePre = Graph.fromEdges(edges, null).connectedComponents().vertices

    //find and set oldest uid every group
    uidChange = setOldestUid(uidChangePre.join(vertices)).cache()

  }

  def save(hourList:String): Unit = {

    val hours = hourList.split(",")
    val (hourSave,limitDown) = (hours.last, ( logtimeFormat.parse( hours.head ).getTime /60000 ).toInt )
    val limitUp = limitDown + 480;

    val uidChangeDic = sc.broadcast(uidChange.map(x => { val (v1, (v2, time)) = x; (v1, v2) }).filter(x => x._1 != x._2).collectAsMap())
    val indexNew = mergeData.map(p => { p.uid = uidChangeDic.value.getOrElse(p.uid, p.uid); p.uidSet = null; p })

    //----------uid to delete in hbase---------
    val oldUidChange = uidChange.filter(x => x._2._2 < limitDown).map(x => { val (v1, (v2, time)) = x; (v1, v2); }).filter(x => x._1 != x._2);
    oldUidChange.map(x => Array(Utils.compressAguid(x._1), Utils.compressAguid(x._2)).mkString("\t")).saveAsTextFile(s"${hbasePath}/$hourSave/verticesDel")

    //----------hbase add ---------
    val hbaseAdd = indexNew.filter(p => limitDown <= p.time && p.time < limitUp).cache() //.map(p=>{if(p.time > limitUp) p.time = limitDown; p})
    hbaseAddFormat(hbaseAdd).coalesce(parallelism).saveAsTextFile(s"${hbasePath}/$hourSave/verticesAdd")

    //----------test---------
    uidChange.map(x => { val (v1, (v2, time)) = x; Array(Utils.compressAguid(v1),Utils.compressAguid(v2),time).mkString("\t") }).saveAsTextFile(s"${hbasePath}/$hourSave/uidChange")
    val add = hbaseAdd.map(p=>(p.uid,p));
    uidChange.filter(x=>x._1 != x._2._1).join(add).saveAsTextFile(s"${hbasePath}/$hourSave/uidChangeJoinAdd")

    //----------save idmap hourly ---------
    hours zip hourRdds map (pair => {
      val (hour, rdd) = pair;
      val res = rdd.coalesce(parallelism, true).map(p => { p.uid = uidChangeDic.value.getOrElse(p.uid,p.uid); p }).cache()
      res.filter(p => p.idType == 1).map(p => Array(p.cid, Utils.compressAguid(p.uid), p.time).mkString("\t")).coalesce(parallelism, true).saveAsTextFile(s"${idmapPath}/$hour/agsid", classOf[GzipCodec]);
      res.filter(p => p.idType != 1 && p.idType != 5).map(p => Array(p.cid, Utils.compressAguid(p.uid), p.idType, p.time).mkString("\t")).coalesce(parallelism, true).saveAsTextFile(s"${idmapPath}/$hour/channelid", classOf[GzipCodec])
    })

    //----------save index---------
    if (!hourSave.endsWith("23")) {
//      indexNew.map(_.toString).saveAsTextFile(s"${historyPath}/$hourSave", classOf[GzipCodec])
    }

  }



  /** **********************************************
    * main
    ***********************************************/
  def main(args: Array[String]): Unit = {

    // sc.stop
    val conf = new SparkConf().setAppName("Idmap-history")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.mb","128")
    //    conf.set("spark.kryo.registrationRequired", "true")
    conf.registerKryoClasses(Array(
      classOf[com.larry.da.jobs.idmap.Person],
      classOf[com.larry.da.jobs.userdigest.UserMapping],//userdigest
      classOf[scala.collection.mutable.WrappedArray.ofRef[_]]
    ))
    sc = new SparkContext(conf)

    //    val args = "2016-01-18-00 - 8 1 150 11".split(" ")
    1 to args.length zip args foreach(println)
    val Array(current,lastHistoryArg,hourSpanStr,runCountStr,parallelismArg,methods) = args
    parallelism = parallelismArg.toInt
    graphParallelism = parallelism/3
    val hourSpan = hourSpanStr.toInt
    val runCount = runCountStr.toInt
    var lastHistory = if( lastHistoryArg.length < 2) "" else lastHistoryArg
    val sdf=new SimpleDateFormat("yyyy-MM-dd-HH");
    val runHour= Calendar.getInstance()
    println("-----------------------")
    println(parallelism)
    println(graphParallelism)
    println(partitionCount)
    println(historyPath)
    println(hbasePath)
    println(idmapPath)
    println(hourlyPath)
    println(hbaseOutPath)
    println(methods)
    println("-----------------------")
    0 until runCount foreach(span => {
      val hourList = new ArrayBuffer[String]
      runHour.setTime(sdf.parse(current))
      runHour.add(Calendar.HOUR,span*hourSpan)
      0 until hourSpan foreach(n=>{
        hourList.append(sdf.format(runHour.getTime))
        runHour.add(Calendar.HOUR,1)
      })
      val lastHourString = if(lastHistory != "") {val tmp = lastHistory;lastHistory= ""; tmp} else {runHour.add(Calendar.HOUR,-1 - hourSpan); sdf.format( runHour.getTime)}
      val hourString = hourList.mkString(",")
      if(methods(0) == '1') com.larry.da.jobs.idmap.IdMapHourly_test.computeHourList(sc,parallelism,hourList)
      if(methods(1) == '1') compute(hourString,lastHourString);save(hourString)
      val endHourOfList = hourList.last
      println("---------------------------------")
      println(hourString + ";" + lastHourString)
      println("endHourOfList is : " + endHourOfList)
      println("---------------------------------")
      if(endHourOfList.endsWith("23")){
        if(methods(1) == '1') com.larry.da.jobs.userdigest.ChannelIdMerge.mergeIdMap(sc,endHourOfList.take(10))
      }
    })
  }


}
