package com.larry.da.jobs.userdigest

import breeze.linalg.max

/**
  * Created by larry on 11/12/15.
  */
class UserMapping(var cid:String="", var uid:String="", var idType:Int=0, var time:Int=0) {

  def merge(p:UserMapping)={
    if(this.time > p.time) this else p
  }

  override def toString() = {
    cid + "\t" +
    uid + "\t" +
    idType + "\t" +
    time
  }

}

object UserMapping{

  def apply(text:String)={
    text.split("\t") match {
      case Array(cid,uid,idType,time) => new UserMapping(cid,uid,idType.toInt,time.toInt)
      case Array(cid,uid,time) => new UserMapping(cid,uid,1,time.toInt)
      case _ => new UserMapping()
    }
  }

}

