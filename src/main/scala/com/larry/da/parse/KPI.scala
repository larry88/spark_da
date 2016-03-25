package com.larry.da.parse

import org.apache.spark.SparkContext

//  import com.agrantsem.dm.util.{LogParseUtil => U}
import scala.collection.mutable.{ArrayBuffer,HashMap}

/**
  * Created by larry on 3/3/16.
  */
object KPI {
  var sc: SparkContext = _

  import com.larry.da.util.{LogParseUtil => U}

  def bidderKPI(): Unit ={
    //  """\D23\D?""".r.findFirstIn("_23fasdf")
    //  import com.agrantsem.dm.util.{LogParseUtil => U}
    val channel = "bid"
    val time ="2016-03-02"
    val rdd = U.dspRdd(sc,"bidder",channel,time)
    // rdd.filter(!_.contains("bidtag=|")).take(10).foreach(println)
    val bidderFields = "userId,usertags,hittags,bidtag".split(",")
    val regkey = """\D12956\D?""".r
    val data = rdd.filter(_.contains("_12956")).flatMap(x=>{
      val d = U.bidderLog(x);
      val Array(cid,usertags,hittags,bidtag) = bidderFields.map(k=>{val v =d.getOrElse(k, ""); if(v=="null") "" else v})
      val res = new ArrayBuffer[String]
      if(regkey.findFirstIn(usertags).getOrElse("") > "") res += "usertag"
      if(regkey.findFirstIn(hittags).getOrElse("") > "") res += "hittag"
      if(regkey.findFirstIn(bidtag).getOrElse("") > "") res += "bidtag"
      res.map(k=>(cid,k) ->1)
    }).reduceByKey(_ + _).map(x=>{
      val((cid,k),num) = x;
      (k,(1,num))
    }).reduceByKey((a,b)=>(a._1 + b._1, a._2 + b._2))

    data.sortByKey().map(x=>{ val(k,(uv,pv)) = x; Array(s"${time}_bidder_${channel}_"+k,uv,pv).mkString("\t") }).collect().foreach(println)

//      .aggregateByKey(new HashMap[String,Int])(
//      (a,v) =>{  a.put(v._1 , v._2 + a.getOrElse(v._1,0)); a;} ,
//      (a,b) => { b.foreach(d=>{ val(k,v) = d; a.put(k,v +a.getOrElse(k,0)); }); a;}
//    ).flatMap(x=>{

  }

  def showKPI(): Unit ={
    //  import com.agrantsem.dm.util.{LogParseUtil => U}
    val channel = "baidu"
    val time ="2016-03-02"
    val showRdd = U.dspRdd(sc,"adshow",channel,time)
    //   rdd.filter(!_.contains("bidtag=null")).take(10).foreach(println)
    val showFields = "guid,bidtag,adxreq".split(",")
    val regkey = """\D12956\D?""".r
    val show = showRdd.filter(_.contains("_12956")).map(x=>{
      val d = U.showLog(x);
      val Array(cid,bidtag,req) = showFields.map(k=>{val v =d.getOrElse(k, "null"); if(v=="") "null" else v})
      val bid = regkey.findFirstIn(bidtag).getOrElse("") > ""
      (req,cid,bid)
    }).filter(_._3).map(x=>(x._1,x._2))


    val clickRdd = U.dspRdd(sc,"tt",channel,time)
    val  click = clickRdd.map(x=>{
      val d = U.ttLog(x);
      val req = d.getOrElse("reqid","null")
      (req,1)
    }).filter(_._1 != "null").reduceByKey(_+_)

    val res = show.leftOuterJoin(click).flatMap(x=>{
      val(req,(cid,click)) = x
      val list = ArrayBuffer(("show",1))
      val clickNum = click.getOrElse(0)
      if(clickNum > 0 ) list += "click" -> clickNum
      list.map(k=>(cid,k._1)->k._2)
    }).reduceByKey(_+_).map(x=>{
      val((cid,k),num) =x
      k -> (1,num)
    }).reduceByKey((a,b)=>(a._1 + b._1,a._2 + b._2))


    res.sortByKey().map(x=>{ val(k,(uv,pv)) = x; Array(s"${time}_${k}_${channel}",uv,pv).mkString("\t") }).collect().foreach(println)



  }



  def pvKPI(): Unit ={

      //    C&A: AG_104415_RTYQ	12956
      //  import com.agrantsem.dm.util.{LogParseUtil => U}
      val time ="2016-03-02"
      val rdd = U.pvRdd(sc,"pv",time).filter(_.contains("AG_104415_RTYQ"))

      val fields = "agsid,url".split(",")
      val data = rdd.map(x=>{
        val d = U.pvLog(x)
        val Array(agsid,url) = fields.map(k=>{val v =d.getOrElse(k, ""); if(v=="null") "" else v})
        val isdsp = if(url.contains("ag_mid")) "ag" else "notag"
        (agsid,isdsp) -> 1
      }).filter(x=> x._1._1 != "null" )

      val res = data.reduceByKey(_+_).map(x=>{
        val((agsid,key),num) = x;
        agsid -> (key,num)
      }).aggregateByKey(new HashMap[String,Int])(
        (a,v) =>{  a.put(v._1 , v._2 + a.getOrElse(v._1,0)); a;} ,
        (a,b) => { b.foreach(d=>{ val(k,v) = d; a.put(k,v +a.getOrElse(k,0)); }); a;}
      ).flatMap(x=>{
        val(cid,dic) = x;
        val allpv = dic.values.sum
        val ag = dic.keys.filter(_ == "ag").size > 0
        dic.put("all",allpv)
        if(allpv == 1){
          dic.put("all_bounce",1)
          if(ag)
            dic.put("ag_bounce",1)
        }
        dic.toArray.map(d=>{
          val (k,v)=d
          k->(1,v)
        })
      }).reduceByKey((a,b)=>(a._1 + b._1,a._2 + b._2))

      res.filter(! _._1.startsWith("no")).sortByKey().map(x=>{ val(k,(uv,pv)) =x; Array(s"${time}_pv_"+k,uv,pv).mkString("\t"); }).collect().foreach(println)

  }


  def check(): Unit ={

    val channel = "baidu"
    val time ="2016-03-02"
    val showRdd = U.dspRdd(sc,"adshow",channel,time)
    //   rdd.filter(!_.contains("bidtag=null")).take(10).foreach(println)
//    val showFields = "guid,bidtag,adxreq,bkid".split(",")
    val showFields = "guid,bidtag,adxreq,bkid".split(",")
    val regkey = """\D12956\D?""".r
    val show = showRdd.filter(_.contains("_12956")).map(x=>{
      val d = U.showLog(x);
      val Array(cid,bidtag,req,bkid) = showFields.map(k=>{val v =d.getOrElse(k, "null"); if(v=="") "null" else v})
      val bid = regkey.findFirstIn(bidtag).getOrElse("") > ""
      (req,cid,bid,bkid)
    }).filter(_._3).map(x=>{
      val (req,cid,bid,bkid) = x;
      (cid,bkid) -> 1
    })

    val data = show.reduceByKey(_+_).map(x=>{
      val((cid,bkid),num) = x;
      bkid -> (1,num)
    }).reduceByKey((a,b)=>(a._1 + b._2,a._2 + b._2))

    data.map(x=>{
      val(bkid,(uv,pv)) = x;
      (pv.toLong/uv,(bkid,uv,pv))
    }).sortByKey(false).take(100).foreach(println)




    //---------------------------------
    val cache1 = showRdd.filter(_.contains("_12956")).filter(_.contains("www.peise.net")).cache
    val fields = "time,guid,bidtag,adxreq,bkid,site".split(",")
    cache1.map(x=>{
      val d = U.showLog(x);
      val Array(time,cid,bidtag,req,bkid,site) = fields.map(k=>{val v =d.getOrElse(k, "null"); if(v=="") "null" else v})
      val bid = regkey.findFirstIn(bidtag).getOrElse("") > ""
      (time,bid,bkid,site)
    }).collect.foreach(println)

  }





}
