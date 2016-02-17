package com.larry.da.jobs.userdigest

/**
  * Created by larry on 15/12/15.
  */

import java.text.SimpleDateFormat

object Config {

  val historyIdMapChannelPath = "/user/dauser/aguid/idmapHistory/channel"
  val historyIdMapAgsidPath = "/user/dauser/aguid/idmapHistory/agsid"
  val historyIdMapUidChangePath = "/user/dauser/aguid/idmapHistory/uidChange"
  val hbasePath = "/user/dauser/aguid/hbase"
  val dayIdMapPath = "/user/dauser/aguid/idmap/"

  val partitionCount = 500

  def timeLimitDown(now:String) ={
    val sdf = new SimpleDateFormat("yyyy-MM-dd");
    sdf.parse(now).getTime / 60000 - 60*24*15
  }

}
