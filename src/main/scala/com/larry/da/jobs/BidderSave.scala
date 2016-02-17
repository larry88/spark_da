package com.larry.da.jobs

import org.apache.spark.{SparkConf, SparkContext}
import com.hadoop.compression.lzo.LzopCodec

/**
 * Created by larry on 15-8-5.
 */
object BidderSave {
  var sc:SparkContext = _

  var parallelism = 135 // sc.defaultParallelism
  val keyDic = Map(
    "ucld" -> Map(
      "show" -> "baidu,tanx,agx",
      "tt" -> "baidu,tanx,agx",
      "bid" -> "baidu,tanx,agx"
    ),
    "aws" -> Map(
      "show" -> "",
      "tt" -> "",
      "bid" -> ""
    )
  )

    /* for test manual parameter
    val keys = keyDic("ucld")
    val time = "2015-10-22"
    val partitionCount = 60
     */
  def joindShowBidderClick(keys:Map[String,String],day:String): Unit = {

      //val time = "2015-09-25-13"
      import java.util.Calendar
      import com.larry.da.util.{LogParseUtil => U}

      val calendar = Calendar.getInstance()
      /** *********  adshow log  **********/
      val showFields = "ssp,adxreq,ua,time,agsid".split(",")
      val showlog = sc.union(
        keys("show").split(",").map(
          channel => U.dspRdd(sc, "show", channel, day).map(_ + s"||ssp=$channel"))
      ).map(x => {
        val d = U.showLog(x);
        val Array(ssp,adxreq,ua,showtime,agsid) = showFields.map(k=> { val v = d.getOrElse(k, "null"); if(v=="") "null" else v })
        //output
        ((ssp,adxreq), (ua,showtime,agsid))
      }).filter(x => x._1._2 != "null").reduceByKey((a,b)=>{if(a._2 < b._2) a else b},parallelism * 3)


      /** *********  bidder log  **********/
      val bidderFields = "ssp,reqId".split(",")
      val bidderlog = sc.union(
        keys("bid").split(",").map(
          channel => U.dspRdd(sc, "bidder", channel, day).filter(_.contains("\"mid\":\"")).map(_ + s"||ssp=$channel")
        )
      ).map(x => {
        val d = U.bidderLog(x);
        val Array(ssp,reqId) = bidderFields.map(k=>{val v =d.getOrElse(k, "null"); if(v=="") "null" else v})
        ((ssp, reqId),x)
      }).filter(x => x._1._2 != "null").reduceByKey((a,b)=>a,parallelism * 3)


      /** *********  tt log  **********/
      val clickFields = "ssp,reqid,time".split(",")
      val ttlog = sc.union(keys("tt").split(",").map(channel => U.dspRdd(sc, "tt", channel, day).map(_ + s"||ssp=$channel"))).map(x => {
        val d = U.bidderLog(x);
        val Array(ssp,reqid,clicktime) = clickFields.map(k=>{val v =d.getOrElse(k, "null"); if(v=="") "null" else v})
        ((ssp, reqid), ("1",clicktime))
      }).filter(x => x._1._2 != "null").reduceByKey((a,b)=>{if(a._2 < b._2) a else b},parallelism * 3)


      /** *********  join(show,tt,bidder) **********/
      val data = showlog.leftOuterJoin(ttlog).leftOuterJoin(bidderlog)
      val data1 = data.map(x => {
        val ((ssp, reqid), (((ua,showtime,agsid), ttTuple), bidTuple)) = x;
//        val click = ttTuple.getOrElse("0")
        val (click,clicktime) = ttTuple match {
          case Some((f1,f2)) => (f1,f2);
          case _ => ("0","null");
        }
        val bidValue = bidTuple.getOrElse("null")
        if(bidValue == "null") "null" else Array(bidValue,"ua="+ua,"click="+click,"clicktime="+clicktime,"agsid="+agsid).mkString("||")
      }).filter(_ != "null")

      /** *********  save **********/
      data1.saveAsTextFile(s"/user/dm/bidlog/$day",classOf[LzopCodec])

  }


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("BidderClickByShow")
    sc = new SparkContext(sparkConf)
    //    val args = "ucld 2015-11-11 135".split(" ")
    val Array(machineRoom,day,partitionCount) = args
    parallelism = partitionCount.toInt
    val keys = keyDic(machineRoom)
    joindShowBidderClick(keys,day)
  }

  def run_history(sc:SparkContext): Unit = {

    parallelism = 150
    16 to 19 foreach(d => {
      val day = "2015-11-" +  { if (d < 10) "0" + d else d.toString }
      joindShowBidderClick(keyDic("ucld"),day)
    })

  }


  def checkData(sc:SparkContext,day:String,w:Int): Unit ={

    import com.hadoop.compression.lzo.LzopCodec
    val log = sc.newAPIHadoopFile(s"/user/dm/bidlog/$day",
        classOf[com.hadoop.mapreduce.LzoTextInputFormat],
        classOf[org.apache.hadoop.io.LongWritable],
        classOf[org.apache.hadoop.io.Text]).map(_._2.toString)
    val data = log.repartition(sc.defaultParallelism * w)
    data.saveAsTextFile(s"/user/dm/bidlog/test_$day",classOf[LzopCodec])

  }


  def tt(sc:SparkContext): Unit ={

    1 to 30 foreach(n=>{
      val day = "2015-09-" + { if (n<10) "0"+n.toString else n.toString }
      checkData(sc,day,3)
    })

    1 to 9 foreach(n=>{
      val day = "2015-10-" + { if (n<10) "0"+n.toString else n.toString }
      checkData(sc,day,2)
    })
  }



}
