package com.larry.da.jobs

/**
 * Created by larry on 15-7-24.
 */


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{Map, Set,ArrayBuffer}

object Static {


  def agsidStatic(sc:SparkContext,time:String): Unit ={

    import com.larry.da.util.{LogParseUtil => U}
    val log = sc.union( "adx,tanx,baidu".split(",").map(channel=> U.dspRdd(sc,"cm",channel,time) ) )

    val repeat = log.map(x=>{
      val m=U.cmLog(x);
      val (ssp,guid,agsid) = (m.getOrElse("ssp","null"),m("guid"),m.getOrElse("agsid","null"))
      ((ssp,guid),Map((agsid -> 1)))
    }).filter(x=>x._1._1 != "null" && x._1._2 != "null" && x._2.keys.head != "null").reduceByKey((a,b)=>{//(guid,Map(agsid,num))
      b.foreach(x=>{
        val (k,v) =x;
        if (a.contains(k)) { val resV = v + 1; a += (k -> resV) }
        else a += (k -> 1)
      })
      a
    }).filter(x=>{
      val((ssp,guid),dic)=x;
      dic.size > 1
    }).flatMap(x=>{ //((key,agsid),num)
    val((ssp,guid),dic)=x;
      dic.keys.flatMap(agsid=>{
        Array(ssp,"allssp").map(k=>(k,agsid))
      })
    }).distinct().map(x=>{
      val(key,agsid) = x;
      (key,1) //(key,num)
      (time + "_" +key + "_wrong" ,1)
    }).reduceByKey(_+_)

    val all = log.map(x=>{
      val m=U.cmLog(x);
      val (ssp,agsid) = (m.getOrElse("ssp","null"),m.getOrElse("agsid","null"))
      (ssp,agsid)
    }).filter(x=>x._1 != "null" && x._2 != "null").flatMap(x=>{
      val(ssp,agsid) =x;
      Array("allssp",ssp).map(k=>(k,agsid))
    }).distinct().map(x=>{
      val(key,agsid) =x;
      (time + "_" +key +"_All",1)
    }).reduceByKey(_+_)

    sc.union(all,repeat).map(x=>{
      val(k,v) = x;
      Array(k,v).mkString(",")
    }).saveAsTextFile("cm/agsidStatic")

  }

  def main(args: Array[String]): Unit = {
    val time = args(0)
    val sparkConf = new SparkConf().setAppName("StaticAgsid")
    val sc = new SparkContext(sparkConf)
    agsidStatic(sc,time)
  }


}
