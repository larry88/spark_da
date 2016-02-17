package com.larry.da.jobs.idmap

/*

import com.larry.da.jobs.idmap.Config
import com.larry.da.jobs.idmap.Utils

 */

import java.text.SimpleDateFormat
import java.util.Date
import breeze.linalg.max
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkContext
import com.google.common.hash.Hashing.md5
import scala.collection.mutable.ArrayBuffer

/**
  * Created by larry on 7/12/15.
  */
object Check {
  var sc:SparkContext = _



  def tt2(): Unit ={

    import com.larry.da.jobs.idmap.Utils
    import com.larry.da.jobs.idmap.Person
    import com.larry.da.jobs.idmap.Config

    val day = "2016-02-15"

    val hourly = sc.textFile(s"/user/dauser/aguid/hourly/${day}-0[0-7]").map(x=>{
      val Array(cidL,uidL,cid,idType,time,num) =x.split("\t")
      val uid = Utils.compressAguid(uidL.toLong)
      Array(cidL,uid,cid,idType,time,num).mkString("\t")
    })

    val newUidChange = sc.textFile(s"/user/peng.liu/aguid/hbase/${day}-07/uidChange").cache()
    val uidChange = sc.textFile(s"/user/peng.liu/aguid/hbase/${day}-07/uidChange").cache()
    val idmapAgsid = sc.textFile(s"aguid/idmap/${day}-0[0-7]/agsid")


//    val Array(dir,file) = "hbase,uidChange".split(",")
    val Array(dir,file) = "hbase,verticesAdd".split(",")
//    val Array(dir,file) = "hbase,verticesDel".split(",")
//    val Array(dir,file) = "history,*".split(",")
//    val Array(me,line) = Array(s"/user/peng.liu/aguid/hbase/${day}-07/$file",s"/user/dauser/aguid/hbase/${day}-07/$file").map(path=>{
//    val Array(me,line) = Array(s"/user/peng.liu/aguid/$dir/${day}-07/$file",s"/user/dauser/aguid/$dir/${day}-07/$file").map(path=>{
    val Array(me,line) = Array(s"/user/dauser/aguid/hbase/${day}-15/$file",s"/user/dauser/aguid/hbase/${day}-15.bk/$file").map(path=>{
      sc.textFile(path)
    }).map(rdd=>{
      rdd.map(line=> {
        val splits = line.split("\t", -1)
        val uid = splits.head
        val ids = splits.tail.map(chn => {
          chn.split(",").map(id => {
            id.split("\\|").head
          }).sortWith((a, b) => a < b).mkString("|")
        }).mkString(";")
        (uid,ids)
      })
    })

    val diff = me.fullOuterJoin(line).map(x=>{
      val(uid,(meids,lineids)) = x;
      val mev = meids match {case Some(v:String)=>v; case _ => "null"}
      val linev = lineids match {case Some(v:String)=>v; case _ => "null"}
      (uid,(mev,linev))
    }).filter(x=>x._2._1 != x._2._2)
    diff.count

    diff.take(100).foreach(println)


  }


  def tt3(): Unit ={
    val Array(me,line) = "aguid/hourly/2016-02-15-10,/user/dauser/aguid/hourly/2016-02-15-10".split(",").map(sc.textFile(_))
//    me.map(d=>(d.split("\t").head,1)).join(line.map(d=>(d.split("\t").head,1))).count
//    me.map(d=>(d.split("\t").head,1)).leftOuterJoin(line.map(d=>(d.split("\t").head,1))).filter(x=>{ x._2._2 == None }).collect().foreach(println)
//    me.map(d=>(d.split("\t")(1),1)).reduceByKey(_+_).map(x=>(x._2,x._1)).sortByKey(false).take(20).foreach(println)

    val Array(m1,l1) = Array(me,line).map(rdd=>{
      rdd.map(d=>{
        val a = d.split("\t")
        ((a(0),a(1),a(2),a(3)),1)
      })
    })
    m1.join(l1).count


    me.map((_,1)).join(line.map((_,1))).count()


    me.filter(_.contains("338333539836370388")).collect().foreach(println)

  }

  def tt4: Unit ={
    val Array(me,line) = "userdigest/export/aws,/tmp/mapred/userdigest/export.bk/aws".split(",").map(sc.textFile(_))
    val Array(m1,l1) = Array(me,line).map(rdd=>rdd.map(x=>{
      val arr = x.split("\t",-1)
      (arr.head,arr.tail.mkString("\t"))
    }))

    val res = m1.fullOuterJoin(l1).filter(x=>{
      val(k,(v1,v2)) =x;
      v1.getOrElse("") != v2.getOrElse("")
    })

    res.take(20).foreach(println)

  }



}
