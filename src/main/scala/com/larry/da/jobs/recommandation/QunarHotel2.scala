package com.larry.da.jobs.recommandation

/**
 * Created by larry on 5/26/15.
 */


import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, Set}




object QunarHotel2 {

  /****************************************************
    *
    * from PV log file
    *
    * ************************************************/
  def parsePvLogFile(sc:SparkContext,time:String): Unit ={
    //********* save log ********
    val log = sc.textFile(s"/user/tracking/pv/log/hourly/TrackingPV_$time*.log.gz").filter(x=>
      x.contains("hotel.qunar.com") && x.contains("dt-")
    )
    val pvData = log.map(x=>{
        val time = x.take(10).substring(2).replace("-", "");
        val agid = x.split("\t")(1);
        val query = x.split("\t")(4)
        val indexCity = query.indexOf("%2Fcity%2F")
        val indexCityEnd = query.indexOf("%",indexCity + 10)
        val indexPid = query.indexOf("%2Fdt-", indexCityEnd)
        val indexPidEnd = query.indexOf("%", indexPid + 6)
        val city = if (indexCity > 0 && indexCity + 10 < indexCityEnd) query.substring(indexCity + 10, indexCityEnd) else ""
        val pid = if (indexPid > 0 && indexPid + 6 < indexPidEnd) query.substring(indexPid + 6, indexPidEnd) else "";
        (time, agid, city, pid)
    })
    val pvRes = pvData.filter(x=>x._3.length > 1 && x._4.length > 0 ).map(x=>{
      val (time,agid,city,pid) = x;
      ((time,agid,Array(city,pid).mkString("_")),1)
    }).reduceByKey((x,y)=>x+y,4).map(_._1)

      /** *******************************
        * time,agsid,pid
        * *******************************/
    pvRes.map( x=> {
      val (time,agid,pid) = x
      Array(time,agid,pid).mkString(",")
    }).saveAsTextFile(s"16hotel/pvlog/$time")


    /** *******************************
      *  ((day,agid,pr),Set(pid))
      * *******************************/
    val data = pvRes.map(x=>{
      val (day,agid,prod)=x;
      val ps = prod.split("_");
      val pid = ps.last;
      val pr = ps.take(ps.length - 1).mkString("_")
      ((day,agid,pr),Set(pid))
    }).reduceByKey((x,y) => x++=y).filter(x=>x._2.size > 1 && x._2.size <= 10).map(x=>{
      val ((day,agid,pr),set) =x
      ((day,agid,pr),set.toArray)
    })


    //binary num
    val rel1 = data.flatMap(binaryRelation _).reduceByKey((x,y) => x+y)


    //save binary relation
    /** *******************************
      * pr1,pid1,pr2,pid2,num
      * *******************************/
    rel1.map(x=>{
      val (((pr1,pid1),(pr2,pid2)),num) = x;
      Array(pr1,pid1,pr2,pid2,num).mkString(",");
    }).saveAsTextFile(s"16hotel/binaryRelation/$time")

  }


  /** **************************************************
    *
    * from Hbase Tag file
    *
    * ************************************************/
  def parseLogFile(sc:SparkContext,time:String): Unit ={
    val log = sc.textFile(s"/user/tracking/userdigest/export/userdigest_export_$time*_all*")
    val data = log.filter(_.contains("R_C_16_HDT_")).map(x=>{
      x.split(",").filter(y=>{
        y.contains("R_C_16_HDT_") || y.contains("aguid")
      })
    })
    val res = data.flatMap(x=>{
      var list = ArrayBuffer[(String,String)]()
      var aguid = ""
      x.foreach(y => {
        try {
          if (y.contains("aguid")) aguid = y.split("aguid\":\"")(1).split("\"")(0);
          else {
            val ss = y.split("R_C_16_HDT_")(1).split("@")(0).split("_")
            val pid = ss.filter(x=>{ !x.contains("-") && !x.contains("%")  } )
            val day = if (y.contains("@")) y.split("@")(1).split("\"")(0).take(6) else "no_time";
            if(pid.length > 1) list += ((day, pid.mkString("_")))
          }
        }catch{
          case e:Exception => {
            println(Array(aguid,y).mkString(","))
          }
        }
      })
      list.map(x=>{
        val (day,pid) = x;
        Array(day,aguid,pid).mkString(",")
      })
    })
    res.saveAsTextFile(s"16hotel/logdata/$time")
  }



  //create binary relation
  def binaryRelation(x:((String,String,String),Array[(String)])) ={
    val ((day,agid,pr),list) = x;
    var res = ArrayBuffer[(((String,String),(String,String)),Int)]()
    val pidList = list.sortWith((a,b) => a.compareTo(b) < 0);
    var(i,j) = (0,0)
    while(i<pidList.length){
      j = i+1;
      while(j<pidList.length){
        val pid1 = pidList(i)
        val pid2 = pidList(j)
        val p1 = (pr,pid1)
        val p2 = (pr,pid2)
        res += ( ((p1,p2),1) );
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
    dayList.foreach(x=>{ if(x.size > 1) parsePvLogFile(sc,x);})


    val rel1 = sc.textFile("16hotel/binaryRelation/*").map(x=>({
      val Array(pr1,pid1,pr2,pid2,num) = x.split(",")
      (((pr1,pid1),(pr2,pid2)),num.toInt)
    })).reduceByKey(_+_)

    //join binary relation and feed data
    val feedFile = sc.textFile("16hotel/16hotelFeed.txt").map(x=>{(x.split(" ")(0),1)})
    val binaryJoined = rel1.flatMap(x=>{
      val (((pr1,pid1),(pr2,pid2)),num) = x;
      val p1 = pr1 + "_" + pid1;
      val p2 = pr2 + "_" + pid2;
      Array((p1,x),(p2,x))
    }).join(feedFile).map(x=>{
      val (pid,(b,onlineCount))=x
      (b,onlineCount)
    }).reduceByKey((a,b)=> a+b).filter(_._2 == 2).map(_._1)


    //sort and take head n
    val rel2 = binaryJoined.flatMap(x=>{
      val(((pr1,p1),(pr2,p2)),num) = x;
      Array(((pr1,p1),ArrayBuffer((pr2,p2,num))),((pr2,p2),ArrayBuffer((pr1,p1,num))) )
    }).reduceByKey((x,y) => x ++= y).map(x=>{
      val((pr,pid),list) = x;
      ((pr,pid),list.sortWith((x,y)=>x._3 > y._3).take(5))
    })

    //format data to save
    rel2.map(x=>{
      val((pr,pid),list) = x;
      pr + "_" + pid + "," +list.map(x=>{val(r,id,num)=x;id}).mkString(",")
    }).saveAsTextFile("16hotel/result1")


    //format data to sav
    rel2.map(x=>{
      val((pr,pid),list) = x;
      pr+"_"+pid + "," +list.mkString(",")
    }).saveAsTextFile("16hotel/result2")


  }
}
