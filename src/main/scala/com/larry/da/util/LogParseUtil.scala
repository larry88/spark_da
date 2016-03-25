package com.larry.da.util

import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.model.Split
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{Map, Set}

import java.util.regex.Pattern;



/**
 * Created by larry on 15-6-18.
 */
object LogParseUtil {



  /** *******************************************************************
    *               RDD
    * dspRdd: logtype: bidder,adshow,cm,tt
    *         channel: baidu,tanx,bid (1 mathine: tanxbid1)
    * ttRdd: logType: pv,track
    * tagRdd : logType: inc,all
    *         channel:baidu,tanx,adx
    * ******************************************************************/
  def dspRdd(sc:SparkContext,logType:String,channel:String,time:String):RDD[String]= {
    val logtype = if (logType == "bidder") "Bidder" else logType
    val logPath = if (logtype == "Bidder") s"/user/dsp/log/*$logtype*$time*$channel*lzo" else s"/user/dsp/log/*$logtype*$channel*$time*lzo"
    sc.newAPIHadoopFile(logPath, classOf[com.hadoop.mapreduce.LzoTextInputFormat], classOf[org.apache.hadoop.io.LongWritable], classOf[org.apache.hadoop.io.Text]).map(_._2.toString)
  }

  def pvRdd(sc:SparkContext,logType:String,time:String):RDD[String]={
    val logPath = if (logType == "pv")
      s"/user/tracking/pv/log/hourly/*$time*.log.gz"
    else
      s"/user/tracking/log/hourly/*$time*.log.gz"
    sc.textFile(logPath)
  }

  def tagRdd(sc:SparkContext,logType:String,channel:String,time:String):RDD[String]={
    val logPath = s"/user/tracking/userdigest/export/userdigest_export_$time*$channel*$logType*.gz"
    val log = sc.textFile(logPath)
    log
  }

  def realTimeRdd(sc:SparkContext,time:String):RDD[String]={
    val logPath = s"/user/tracking/userdigest/log/hourly/realtime_tags_$time*.gz"
    val log = sc.textFile(logPath)
    log
  }




  /** *******************************************************************
    *             Parse LOG
    * ******************************************************************/

  val splitPatternAll = Pattern.compile("""\|~\||\|\||&""")
  val splitPattern = Pattern.compile("""\|~\||\|\|""")
   def toDictionary(txt:String)={
    val time = txt.take(19);
    val tuple = splitPattern.split(txt,0).tail.map(x=>{
      val items = x.split("=",2)
      if (items.length > 1){
        (items(0),items(1))
      }
      else
        (x,"")
    })
    val map = Map(tuple toSeq :_*)
    map += ("time" -> time)
  }

  def toDictionary2(txt:String,splitStr:String)= {
    val tuple = txt.split(splitStr).map(x => {
      val items = x.split("=",2)
      if (items.length > 1) {
        (items(0), items(1))
      }
      else
        ("", "")
    }).filter(_._1 != "")
    Map(tuple toSeq: _*)
  }
//  def toDictionary_bk(txt:String)={
//    val time = txt.take(19);
//    val tuple = txt.split("""\|~\||\|\||&""").tail.map(x=>{
//      val items = x.split("=")
//      if (items.length > 1){
//        val key = if (items(0).endsWith("google_gid")) "guid" else items(0)
//        val key1 = key.split("""\|""").last
//        (key1,items.tail.mkString("="))
//      }
//      else
//        (x,"")
//    })
//    val map = Map(tuple toSeq :_*)
//    map += ("time" -> time)
//  }
   def showLog(txt:String)={
     var map = toDictionary(txt);
     //deal agsid=null in baidu show
     val cookie = map.getOrElse("cookie","null")
     if (map.getOrElse("agsid","null") == "null" && cookie.contains("agsid=")){
       val items=cookie.split("agsid=").filter(i=>i.size > 3)
         if(items.size > 0){
           val agsid = items.last.split(";")(0)
           map += ("agsid" -> agsid)
         }
     }
     if(!map.contains("guid") && map.contains("requestUrl")){
        val dic = toDictionary2(map("requestUrl"),"&")
        Array("guid").foreach(k=>{
          if(dic.contains(k)) {
            val v = dic(k);
            val resV = if(v=="") "null" else v;
            map +=(k -> resV)
          }
        })
     }
//     Array("agsid","guid").foreach(k=>{
//       if(!map.contains(k)) map +=(k -> "null")
//     })
     map
  }
  def cmLog(txt:String)={
    val d = toDictionary(txt);
    val target = d.keys.filter(x =>x.startsWith("Matching ") && x.endsWith("google_gid"))
    if(target.size > 0){
      val k = target.head;
      d += ("guid" -> d(k))
    }
    d
  }

  def ttLog(txt:String)={
    var map = toDictionary(txt);
    //deal agsid=null in baidu show
    val cookie = map.getOrElse("cookie","null")
    if (map.getOrElse("agsid","null") == "null" && cookie.contains("agsid=")){
      val items=cookie.split("agsid=").filter(i=>i.size > 3)
      if(items.size > 0){
        val agsid = items.last.split(";")(0)
        map += ("agsid" -> agsid)
      }
    }
    if(!map.contains("guid") && map.contains("refurl")){
      val dic = toDictionary2(map("refurl"),"&")
      Array("guid").foreach(k=>{
        if(dic.contains(k)) {
          val v = dic(k);
          val resV = if(v=="") "null" else v;
          map +=(k -> resV)
        }
      })
    }
    map
  }
  def bidderLog(txt:String)={
    var map = toDictionary(txt);
    val ss = map.getOrElse("Threadid","").split(",")
    if(ss.length > 1){
      map += ("Threadid" -> ss.head)
      ss.tail.foreach(x=>{
        val items = x.split("=")
        if (items.length > 1){
          map +=(items(0) -> items.tail.mkString("="))
        }
      })
    }
    Array( ("userId","guid"),("agUserId","agsid") ).foreach(x=>{
      val(k,v) = x;
      map += (v -> map.getOrElse(k,"null") )
    })
    map
  }



  //pv log
  val pvFields="time,ip,agsid,tid,url,query,useragent".split(",")
  val oldFields="time,ip,custid,eventid,agsid,evtp1,evtp2,evtp3,evtp4,tid,url,cookie,query,useragent,creative,keyword".split(",")
  def pvLog(txt:String,parseQuey:Boolean = false)={
    val item = txt.split("""\|~\||\t""")
    val tuple = if(item.length == 7) pvFields zip item else oldFields zip item

    if(parseQuey){
      val querys = tuple.filter(p=>p._1 == "query")
      val query = if(querys.size > 0) querys.head._2 else ""
      val tuple2 =  query.split("&").map(_.split("=",2)).filter(_.size > 1).map(x=>x(0) -> x(1))
      Map( (tuple ++ tuple2 ) toSeq : _* )
    }else{
      Map(tuple toSeq :_*)
    }
  }




  /***********************************************
    * bidder_control state RDD
    * *******************************************/
  def stateRdd(sc:SparkContext,dir:String)={
    val newestStatePath = if (dir == ""){
      val stateDir = "/user/dsp/bidder_control/state"
      val statePrefix = "state." //save state file prefix
      new FileGetter(sc,stateDir,statePrefix).getNewestFile()
    }else dir;

    val rdd = sc.textFile( newestStatePath ).map( x=>{
      val ss = x.replace(",List(",",").replace("(","").replace(")","").replace(" ","").split(",")
      val Array(agsid,sendType,sendNum)=ss.take(3)
      val item = ss.drop(3)
      var(num,time)=(0,"")
      val set:Set[(Int,String)] = Set();
      0 to item.length -1 foreach(i=>{ if(i % 2 == 0) num = item(i).toInt else {time=item(i);set +=( (num,time) );} })
      (agsid,(sendType,sendNum.toInt,set))
    }).reduceByKey(
        (a,b)=> (if(a._1 != "")a._1 else b._1,if(a._2 > 0)a._2 else b._2,a._3 ++ b._3), sc.defaultParallelism
    ).map(x=>{
      val(agsid,(sendType,sendNum,set))=x;
      val arr = set.toArray.sortWith((a,b)=>a._1 > b._1).sortWith((a,b)=>a._2 > b._2)
      var t=arr.head;
      val resArr = arr.filter(p=>{
        if(p._2 != t._2) {t = p;true}
        else{p._1 > t._1}
      })
      (agsid,(sendType,sendNum,resArr))
    })

    rdd
  }














  def test(sc:SparkContext): Unit ={


    //cm
    val txt = "2015-06-17 00:00:00|~|Matching TANX Cookie: google_gid=WV5RGy_MvQs=||google_cver=1||agsid=GGU0sGecyxqe7aSC||isSetAgSid=false||ip=111.206.36.6||ua=Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11||refurl=http://lf.58.com/zpshengchankaifa/20843583639946x.shtml?PGTID=14341905203430.6391751891933382&ClickID=1||cookie=agsid=GGU0sGecyxqe7aSC||querystr=tanx_ver=1&atscu=AG_180113_IPDW&tanx_tid=WV5RGy_MvQs%3D"

    val txt1 = "2015-06-17 04:00:28|~|183.71.169.38|~|354|~|1|~|41t4pSeyQVsgqoQT|~|354-1-9a8f905024f0e1ee.65fc8ab13ecb547a_m|~||~||~||~||~|http://m.baidu.com/baidu.php?sc.Ks0000KJdc9VyGNWkH6Fso4p9GtPjZopcuMnIjeDvUd1AYhJsypM9ds6yhDxtXXQWBMSXYVx4bFrMHfJUZEO63G2M5L2YWcXSceYa0d3iyty1B1U3jXeGgCo_F7Wj5HPojqjKvt.7D_iR5FMNn17fWHGwKfCiqZXZKAkkLIMNrFBEoWm3e815gML_ePXS1IubePIBpYGt8MIo9vnLdtRsLe81ku4I5yrp7Wdukmr8-P1tA-WtL4E9lO-Hf3ZHIo7xgV3An-IhOIEzU2e5ZJ1TkYqMAEu8_LeVOgHfmzIyTUAh9gd8mJCRnXZB19zptrHgN6ITeP4g_3_anrHgYmcovgmThEjkd1FdGhstUJt-I-XkmhZm3le2t--xmz_nYQZHvyNtN0.U1Yk0ZDqdPJtY_L3zTSt4Vps0ZfqzrXl1_L3zTOgonX80A-V5HcznsKM5yPdpyw-Xj60Iybq0ZKGujY1nsKWpyfqPjfY0AdY5HcsPHFxnH0kPdtznjDk0AVG5Hc0TMfqPjfY0ANGujYknj7xPj0Y0AFG5HKxnW0snjFxnW0sn1D0UynqnHDsrj03P1TdrNtdPHcvn1cYr7ts0Z7spyfqn0Kkmv-b5H00ThIYmyTqn0KEIhsqnH0snj0sPaYkn7tknj0snj0vQH-xnH0kPzYsg1DsPjnVnNtknjR4Qywlg1DknjRVnNtknHTkQH7xnHDLPiYkg1DkrHmVnNtknWDVnNtknWfVuZGxnHcdnBYkg1DzP1TVuZGxnHc3nidbX-tkn10sQywlg1D1njmVuZGxnHnsPzdbX-tkn104Qywlg1cYPYsVuZGxPHnsQywlg1R1nBdbX-tLnjDVn7tLn1TVPsK9mWYsg100ugFM5H00TZ0qn0K8IM0qna3snj0snj0sn0KVIZ0qn0KbuAqs5H00ThCqn0KbugmqIv-1ufKhIjYz0ZKC5H00ULnqn6KBI1YY0A4Y5HD0TLCqnHnzn7tknj0k0ZwdT1YkPHRLP1c4PHT1nWbsPWTkrjD1P0Kzug7Y5HDYn1fYrjf3nWf3rjf0mv6qUZNxIv-1ufKYIHddnWDsPHc1P0K15H7hmynYuhcznH79rHRLm160ph_qPHDYm1nsPHmdrAmvm1u9mfKYUHYknjRLnHbY0APh5HD0Thcqn0K_IyVG5Hc0mv4YUWdMmy4JpyPEUNqWTZc0TLPs5HD0TLPsnWYz0ZNzUjdCIZwsrBtEnvT8uv78phb8mvqVQvFJgvGlXZN-Tv9-UhTEnHR3njTzrHmYng60Tv-b5HIBPWc4Pvu-Pvnzm1FbmH60mynqnfKBUjYs0APzm1YkrjmvP0&qid=7b6297fe7c2c2da8&sourceid=111&placeid=1&rank=1&shh=m.baidu.com&word=%E5%9C%A8%E5%AE%B6%E7%BD%91%E4%B8%8A%E5%85%BC%E8%81%8C&sht=1012014a&ck=1197.129.234.165.0.0|~|-|~|cus=354&eid=1&p=354-1-9a8f905024f0e1ee.65fc8ab13ecb547a_m&src=Baidu&medium=PPC&Network=1&kw=21095928107&ad=6431850959&mt=2&ap=mt1&d=http%3a%2f%2fhuodong.baidu.com%2fxunbao%2f%3frefer%3d121018|~|Mozilla/5.0 (Linux; U; Android 4.2.2; zh-cn; G958 Build/JDQ39) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30|~|6431850959|~|21095928107"
    val txt2 = "2015-06-17 04:02:43|~|222.44.97.62\tGbtnUuEg4TDh5XmT\t\thttp://s324.cqby.xy.com/\tatscu=AG_354007_OJNO&atsdomain=xy.com&atsev=101&atsusr=86f9d1d5d8894ffee48fab50a8b2b0b4&agfid=gfT2TTxM8bEkc0pF&atsp=Win32&atsl=zh-CN&atsbr=1212x684&atssr=1366x768&atsc=utf-8&atsh=s324.cqby.xy.com&atsrf=http%3A%2F%2Fcqby.xy.com%2F&atspv=1&atstime=1434484963884\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)"
    //show baidu
    val txt3 = "2015-06-17 04:00:00|~|AdShowRequest: src=BAIDU||adxreq=15061703595814ed906C29D9334A6CDF||price=40||adid=2299_71f99648ec802ec6||ideaid=null||adxgid=null||bidprice=null||site=xa.bendibao.com||bidtime=1434484798||vcode=24EC147B||bidtag=null||click=||requestUrl=http://ad.agrantsem.com/AdServer/BaiduShowNotice?reqid=292be0361b538627&price=VYCAPgAJlvF7jEpgW5IA8oGDw6PYFqZVUMhlrA&ext_data=15061703595814ed906C29D9334A6CDF%3D2299_71f99648ec802ec6%3Dxa.bendibao.com%3D1434484798%3D24EC147B%3D0%3D0%3D1%3D9223372032561140466%3Dnull%3D5070%3DA%3D25ff356102c4d2b1d5798cd5d1c909c8%3D10||ip=121.204.128.93||agsid=null||ua=Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko||refurl=http://pos.baidu.com/acom?adn=0&at=128&aurl=&cad=1&ccd=24&cec=utf-8&cfv=18&ch=0&col=zh-CN&conOP=0&cpa=1&dai=5&dis=0&layout_filter=rank&ltr=http%3A%2F%2Fwww.baidu.com%2Flink%3Furl%3DhcYZYAqYNmMLOwJmtE-4cKtQuvMwnL2pWJiGXUXKf4e4lZ40bSRB66HO3yaR9Xm8D7Bh1yqSSNoZR5Febynv0a%26ie%3Dutf-8%26f%3D8%26tn%3D56060048_4_pg%26wd%3D%25E5%25A6%2582%25E4%25BD%2595%25E5%258A%259E%25E7%2590%2586%25E8%25A5%25BF%25E5%25AE%2589%25E6%2597%2585%25E6%25B8%25B8%25E5%25B9%25B4%25E7%25A5%25A8%26inputT%3D4477&ltu=http%3A%2F%2Fxa.bendibao.com%2Ftour%2F2015113%2F49677.shtm&lunum=6&n=haohu_cpr&pcs=1079x481&pis=10000x10000&ps=2027x39&psr=1093x614&pss=1080x2138&qn=0e7a65f24410610f&rad=&rsi0=650&rsi1=90&rsi5=4&rss0=&rss1=&rss2=&rss3=&rss4=&rss5=&rss6=&rss7=&scale=&skin=tabcloud_skin_2&stid=5&td_id=1331954&tn=baiduCustSTagLinkUnit&tpr=1434484790835&ts=1&version=2.0&xuanting=0&dtm=BAIDU_DUP2_SETJSONADSLOT&dc=2&di=u1331954&ti=%E5%A6%82%E4%BD%95%E8%B4%AD%E4%B9%B0%E9%99%95%E8%A5%BF%E6%97%85%E6%B8%B8%E5%B9%B4%E7%A5%A8-%20%E8%A5%BF%E5%AE%89%E6%9C%AC%E5%9C%B0%E5%AE%9D&tt=1434484793750.4039.4282.4282||cookie=null||width=0||height=0||vs=1||bkid=9223372032561140466||bt=A||region=5070||baseprice=500000||markup=35||s=10"


    val txt4="2015-06-23 04:00:00|~|Threadid=11407,reqId=150623040000deacdBA425E3F8D2A193,reqNo=736799480,ptime=3,1,0,1,0,1,0,0,0,0,0|~|userId=ojeHe_RFfas=|~|agUserId=RQcjtG2ENXmXXscA|~|url=http://images.sohu.com/ytv/BJ/BJSC/30025020140521165734.html?ref=http%3A//tv.sohu.com/20150620/n415387249.shtml&clickping=http%3A//vg.aty.sohu.com/pclick%3Fc%3D1%26s1%3D%26v1%3D1053%26v2%3D1316%26p%3Dcp_102_1%26loc%3DCN3410%26ac%3D12210%26ad%3D154336%26pt%3D16582%26b%3D197611%26bk%3D63268091%26at%3D0%26du%3D0%26al%3D8441403%26out%3D0%26au%3D%26vid%3D2429142%26qd%3D%26rt%3De73f4849a02d3a25%26uv%3D14200792786343764579%26vt%3Dvrs%26rd%3Dtv.sohu.com%26fee%3D0%26isIf%3D2%26suv%3D1412290926094710%26scookie%3D1%26ar%3D14%26url%3Dhttp%3A//tv.sohu.com/20150620/n415387249.shtml%26pagerefer%3D%26sign%3DCDQFtRcjvEY8I_jnK6UbGF6G7QD0WWiC%26rip%3D60.172.97.178%26sip%3D10.10.77.100|~|ip=60.172.97.178|~|region=9132|~|city=黄山|~|adslots=[{\"slotId\":0,\"w\":300,\"h\":250,\"vs\":0,\"tp\":3,\"min_cpm\":50000,\"bkid\":\"mm_26632206_2690592_21024578\"}]|~|ads=[]|~|hittags=[]|~|usertags=[\"R_VS_16@150621\",\"R_C_16_HAREA_9132_2015-06-02_2015-06-03@1506011500\",\"R_C_16_HAREA_9132_2015-06-20_2015-06-21@1506201811\",\"DI_0401_1@150621\",\"R_C_16_H@150621\",\"M\",\"R_C_16_HDT_huangshan_2117_2015-06-20_2015-06-21@1506201811\",\"R_C_16_HDT_huangshan_2117_2015-06-02_2015-06-03@1506011500\",\"R_C_16_LS@1506201810\",\"DI_0204_1@150621\"]|~|svs=[\"40102:1.0\"]|~|bidtag=|~|bl=0|~|bt=A|~|fffd=11000|~|prcs=0.5.4.0|~|uc=|~|gender=|~|level=1,2,1|~|adprice=|~|hiscpm=|~|hiswin=|~|hisctr=|~|dt=D|~|hotelPriceAdjustA=|~|cbid=0a67267d00005588693f4e62116742c1|~|pf=|~|isapp=0|~|lukws=[]|~|bidkw="


  }




}
