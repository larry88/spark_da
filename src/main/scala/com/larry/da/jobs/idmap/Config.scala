package com.larry.da.jobs.idmap

import java.nio.charset.Charset

import scala.io.Source

/**
  * Created by larry on 14/12/15.
  */
object Config {
  val chaset_utf8 =  Charset.forName("utf-8")
  var partitionCount = 1000

//  pvdir=/user/dauser/dataSource/{YYYY-MM-DD-HH}*
//  pvdir_done=/user/dauser/dataSource/{YYYY-MM-DD}*/_successful
//  pvdir_done=/user/dauser/dataSource/{YYYY-MM-DD}*done


  def getDataSource()= {
      Source.fromFile("config/config.properties").getLines
        .filter(x=> ( !x.startsWith("#") ) &&  x.trim != "")
        .map( line => line.split("=").map(_.trim) )
        .map( x => (x(0), x(1))
      ).filter(!_._1.endsWith("_done")).map(_._2).toArray
  }

}
