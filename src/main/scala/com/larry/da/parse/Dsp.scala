package com.larry.da.parse

import com.larry.da.util.{LogParseUtil => U}
import org.apache.spark.SparkContext

/**
  * Created by larry on 24/2/16.
  */
class Dsp {
  var sc: SparkContext = _

  def TagStatic(): Unit ={

    def tt3(): Unit ={

      val rdd = U.dspRdd(sc,"bidder","adx","2016-02-24")
      val bidderFields = "userId,usertags".split(",")
      val data = rdd.map(x=>{
        val d = U.bidderLog(x);
        val Array(cid,tags) = bidderFields.map(k=>{val v =d.getOrElse(k, ""); if(v=="null") "" else v})
        val tagList = tags.replace("[","").replace("]","").replace("\"","")
        val hastag = if (tagList.length > 0) "hastag" else "notag"
        val hasNewTag = if(tagList.contains("{X")) "newtag" else "noNew"
        ((hastag,hasNewTag,cid),1)
      })

      val res = data.reduceByKey(_+_).flatMap(x=>{
        val ((hastag,hasNewTag,cid),num) = x
        Array(
          ("all",(1,num)),
          (hastag,(1,num)),
          (hasNewTag,(1,num))
        )
      }).reduceByKey((a,b)=>(a._1 + b._1,a._2 + b._2)).collect().map(x=>{
        val(key,(uv,pv))=x
        Array(key,uv,pv).mkString(",")
      }).foreach(println)
    }

  }

}
