package com.larry.da.jobs.userdigest

import org.apache.spark.SparkContext

/*

import com.larry.da.jobs.userdigest.UserMapping;
import com.larry.da.jobs.userdigest.Config

*/

/**
  * Created by larry on 14/1/16.
  */
object test {
  var sc : SparkContext = _
  def tt(): Unit ={
    val day = "2016-01-17"
    val idType = "agsid"
    val Array(me,line) = Array(s"aguid/idmapHistory/$idType/$day",s"/user/dauser/aguid/idmapHistory/$idType/$day").map(path=>sc.textFile(path).map(UserMapping(_)).map(u=>(u.cid,u)))
    me.join(line).count
    me.join(line).filter(x=>x._2._1.uid != x._2._2.uid).count

  }

}
