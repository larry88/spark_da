package com.larry.da.jobs.recommandation

/**
 * Created by larry on 5/26/15.
 */

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object YL2 {


  //parseLogFile(sc,"2015-05-27")
  def parseLogFile(sc:SparkContext,time:String): Unit ={
    //********* save log ********
    val log = sc.textFile(s"/user/tracking/pv/log/hourly/TrackingPV_$time*.log.gz")
      .filter(x=> x.contains("228") && x.contains("ticket-")).map(x=>{
      val time = x.take(19);
      val agid = x.split("\t")(1);
      val pid = x.split("ticket-")(1).split("\\.")(0);
      ((time,agid,pid),1)
    }).reduceByKey((x,y)=>x+y,4).map(x=>{
      val ((time,agid,pid),num) = x
      Array(time,agid,pid).mkString(",")
    })
    log.saveAsTextFile(s"228/logdata/pv228_$time")
  }
//
//  def parseLogFile_16hotel(sc:SparkContext): Unit ={
//    val number = """\d+""".r
//    val log = sc.textFile("/user/tracking/userdigest/export/userdigest_export_2015-06-01_*_inc*")
//    val data = log.filter(_.contains("R_C_16_HDT_")).map(x=>{
//      x.split(",").filter(y=>{
//        y.contains("R_C_16_HDT_") || y.contains("aguid")
//      })
//    })
//    data.flatMap(x=>{
//      var res = ArrayBuffer[(String,String)]()
//      var aguid = ""
//      x.foreach(y=>{
//        if (y.contains("aguid")) aguid = y.split("aguid\":\"")(1).split("\"")(0) ;
//        else {
//          val pid = y.split("R_C_16_HDT_")(1).split("[_@]").filter(number.pattern.matcher(_).matches)(0)
//          val day = y.split("@")(1).split("\"")(0)
//          res += ((day, pid))
//        }
//      })
//      res.map(x=>{
//        val (day,pid) = x;
//        Array(day,aguid,pid).mkString(",")
//      })
//    }).saveAsTextFile("16hotel/logdata/2015-06-01")
//  }



  //create binary relation
  def binaryRelation(x:((String,String),ArrayBuffer[(String,Int)])) ={
    val ((day,agid),list) = x;
    var res = ArrayBuffer[(((String,Int),(String,Int)),Int)]()
    val pidList = list.sortWith((a,b) => a._1.compareTo(b._1) < 0);
    var(i,j) = (0,0)
    while(i<pidList.length){
      j = i+1;
      while(j<pidList.length){
        res += ( ((pidList(i),pidList(j)),1) );
        j+=1;
      }
      i+=1
    }
    res
  }




  /*******************************************
    * main(Array("2015-05-27","2015-05-28"))
    * ****************************************/
  def main(args:Array[String]): Unit ={
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    //******create log data for 2 day : today and yestoay********
    val dayList = args(0).trim.split(",");
    dayList.foreach(x=>{ if(x.size > 1) parseLogFile(sc,x);})


    //********* joined data ********
    val txt = sc.textFile("228/logdata/*").map(_.split(",")).filter(_.length == 3)
    val pidOnline = sc.textFile("228/pidList.txt").map((_,0))
    val joinedData = txt.map(x=>{val Array(time,agid,pid)=x; (pid,(time.split(" ")(0),agid))}).
      join(pidOnline).map(x=>{val(pid,((day,agid),a)) = x; (day,agid,pid)})

    // uniq (day,agid,pid)
    val item = joinedData.map(x=>{val (day,agid,pid)=x; ((day,agid,pid),1)}).reduceByKey((x,y) => x+y)
    //pids to list
    val data = item.map(x=>{val((day,agid,pid),n1)=x; ((day,agid),ArrayBuffer(pid)) } ).reduceByKey((x,y) => x ++= y)


    // compute pid total pv
    val pidNum = data.filter(x=>{
      val ((day,agid),pidList) = x;
      pidList.length > 1 && pidList.length <= 7;
    }).flatMap(x=>{
      val ((day,agid),pidList) =x;
      pidList.map(pid=>{
        (pid,(1,ArrayBuffer((day,agid))) )
      })
    }).reduceByKey((x,y)=> (x._1 + y._1,x._2 ++ y._2))


    //pids to list
    val pidList = pidNum.flatMap(x=>{
      val (pid,(num,list) ) = x;
      list.map(y=>{
        val(day,agid) = y;
        ( (day,agid),ArrayBuffer( (pid,num) ) )
      })
    }).reduceByKey((x,y) => x ++ y)

    //binary num
    val rel1 = pidList.filter(x=>{
      val ((day,agid),pidList) = x;
      pidList.length > 1 && pidList.length <= 7;
    }).flatMap(binaryRelation _).reduceByKey((x,y) => x+y)



    //sort and take head n
    val rel2 = rel1.flatMap(x=>{
      val(((p1,p1Num),(p2,p2Num)),number) = x;
      val num = number.toDouble
      Array((p1,ArrayBuffer((p2,num/p2Num))),(p2,ArrayBuffer((p1,num/p1Num))) )
    }).reduceByKey((x,y) => x ++= y).map(x=>{
      val(pid,list) = x;
      (pid,list.sortWith((x,y)=>x._2 > y._2).take(10))
    })

    //format data to save
    rel2.map(x=>{
      val(pid,list) = x;
      pid + "," +list.map(x=>{val(tid,num)=x;tid}).mkString(",")
    }).saveAsTextFile("228/result1")


    //format data to save
    rel2.map(x=>{
      val(pid,list) = x;
      pid + "," +list.mkString(",")
    }).saveAsTextFile("228/result2")

  }

}
