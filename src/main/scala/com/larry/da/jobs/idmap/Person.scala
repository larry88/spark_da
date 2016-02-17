package com.larry.da.jobs.idmap

import com.google.common.hash.Hashing.md5
import scala.collection.mutable.HashMap

/**
  * Created by larry on 4/12/15.
  */
// sc.textFile(path).map(_.split("\t")).map(x => { val Array(guidL,uidL,guid,idType,time,num) =x;
//(guidL.toLong, (uidL.toLong,guid,idType,time.toInt,num.toInt)) })
//class Person(cidL:Long=0,uid:Long=0,cid:String="",idType:Int= -1,time:Int=0,num:Int=0) { }
class Person(var cidL:Long=0,var uid:Long=0,var cid:String="",var idType:Int= 0,var time:Int=0,var num:Int=0) {

  var uidSet:HashMap[Long,Int] = _

  def merge(p:Person,isMergeUid:Boolean = false)={

    if(isMergeUid){
      if(uidSet == null) uidSet = HashMap{this.uid -> this.time}
      if(p.uidSet == null) {
        val t = this.uidSet.getOrElse(p.uid,p.time)
        this.uidSet += ( (p.uid, if(t < p.time) t else p.time) ) // time use past
      }else{
        p.uidSet.foreach(item=>{
          val(k,v) = item
          val t = this.uidSet.getOrElse(k,v)
          this.uidSet += ( (k, if(t < v) t else v) )// time use past
        })
      }
    }

    //    this.time = max(this.time,p.time)
    this.time = if(this.time > p.time) this.time else p.time
    this.num += p.num

    this
  }

  override def toString() = {
    cidL.toString + "\t" +
    uid + "\t" +
    cid + "\t" +
    idType + "\t" +
    time + "\t" +
    num
  }
}


object Person{
  def apply(text:String)={
    text.split("\t") match {
      case Array(cidL,uid,cid,idType,time,num) => new Person(cidL.toLong,uid.toLong,cid,idType.toInt,time.toInt,num.toInt)
      case Array(     uid,cid,idType,time,num) => new Person(md5.hashString(cid,Config.chaset_utf8).asLong(),uid.toLong,cid,idType.toInt,time.toInt,num.toInt)
      case _ => new Person()
    }
  }
}

