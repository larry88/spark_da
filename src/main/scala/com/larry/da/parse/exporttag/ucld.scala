package com.larry.da.parse.exporttag

import com.larry.da.jobs.idmap.Utils
import org.apache.spark.SparkContext
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer,HashSet}
import com.google.common.hash.Hashing.md5

/**
  * Created by larry on 4/1/16.
  */
class ucld {
  var sc:SparkContext = _;

  def tt(): Unit ={

    val log = sc.textFile("/user/tracking/userdigest/export/userdigest_export_2015-12-30_ucld_inc.txt.gz")

    val data = log.flatMap(x=>{
      val Array(aguid,tag,agsid,googleid,baiduid,tanxid,agfid) =x.split("\t",7);
      val res = ArrayBuffer(aguid + "\t" + tag)
      Array(agsid,googleid,baiduid,tanxid,agfid).filter(_ != "").foreach(x=>res.append(x + "\t" +aguid))
      res
    })

    data.saveAsTextFile("tmp/aguid")

  }

  def tt1(): Unit ={

    val log = sc.textFile("/user/dauser/aguid/hbase_output/2016-01-05").map(_.split("\t")).map(x => {
      val Array(uid,cid,idType,time,num) =x;
      (md5.hashString(cid).asLong(), (Utils.unCompressAguid(uid),cid,idType.toInt,time.toInt,num.toInt))
    })

    val data = log.map(x=>{
      val(guidL,(uid,guid,idType,time,num)) = x;
      (uid,idType)
    }).aggregateByKey(new mutable.HashSet[Int]())(
      (a,v) => a += v,
      (a,b) => a ++= b
    )

    val data1 = data.flatMap(x=>{
      val(uid,set) =x;
      val key = set.toArray.sortWith((a,b)=>a<b).mkString("|")
      Array((key,1),("all",1))
    }).reduceByKey(_+_)

    data1.sortByKey().collect().foreach(println)


  }


}
