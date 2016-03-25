package com.larry.da.parse

import java.util

import org.apache.spark.SparkContext
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * Created by larry on 15-7-17.
 */
object PV {
  var sc:SparkContext = _ ;
  import com.larry.da.util.{LogParseUtil => U}



  def tt10(): Unit ={

    //  import com.agrantsem.dm.util.{LogParseUtil => U}
    val log = U.pvRdd(sc,"tracking","2016-03-16")
//    log.map(_.split("""\|~\||\t""",-1).length).map((_,1)).reduceByKey(_+_).collect.foreach(println)

    val data = log.filter( x=>{
      val arr = x.split("""\|~\||\t""",-1)
      val yes =  if(arr.size == 7 && x.contains("ag_kwid")) 1 else if(arr.size == 16) 1 else 0
      yes == 1
    }).map(x=>{
      val d = U.pvLog(x)
      ("all",d.getOrElse("agsid",null)) -> 1
    }).reduceByKey(_+_).map(x=>{
      val((key,agsid),num) =x;
      key ->(1,num)
    }).reduceByKey((a,b)=>(a._1 + b._1,a._2 + b._2))

   data.collect().foreach(println)

    val a =  ArrayBuffer("3")


  }

  def tt1(): Unit ={

    val log = U.pvRdd(sc,"tracking","2016-01-20")
    val pre = log.filter(x=>x.split("""\|~\||\t""",-1).length > 8).map(U.pvLog(_)).map(d=>d.getOrElse("agsid","null")).filter(_ != "null")
    val post = log.filter(x=>x.contains("atstd=") && !x.contains("atstd=&")).map(U.pvLog(_)).map(d=>d.getOrElse("agsid","null")).filter(_ != "null")
    val (preCount,postCount,allCount) = (pre.distinct().count(),post.distinct().count(), pre.union(post).distinct().count())

  }

  def tt2(): Unit ={
    import com.larry.da.util.{LogParseUtil => U}
    val zdd = U.dspRdd(sc,"show","zdd","2016-01-20").map(U.showLog(_)).map(d=>d.getOrElse("agsid","null")).map((_,1))
    val uid = sc.textFile("/user/dauser/aguid/idmapHistory/agsid/2016-01-20").filter(_.startsWith("Z")).map(_.split("\t").take(2)).map(x=>{val Array(agsid,uid)=x;(agsid,uid)})

    val(zddCount,zddDistinctCount,uidCount,joinCount ) = (zdd.count(),zdd.distinct().count,uid.count(),zdd.join(uid).count())

    zdd.join(uid).map(x=>x._1).distinct().count

  }


  def tt4(): Unit ={

    val dic = sc.broadcast( sc.textFile("228/feed.txt").map(_.split("\t")).map(arr=>(arr(0),arr.tail.mkString("|"))).collectAsMap() )

    import com.larry.da.util.{LogParseUtil => U}
    val log = U.pvRdd(sc,"pv","2016-01-26").filter(_.contains(".228.com.cn"))
    val pvFields = "agsid,url".split(",")
    val keyWords = "/shopCart,/cart,/pay".split(",")
    val data = log.map(U.pvLog(_)).map(d=> {
      val Array(agsid, url) = pvFields.map(f => { val v = d.getOrElse(f, "null"); if (v == "") "null" else v })
      val pv2 = keyWords.filter(url.contains(_)).size
      def getProductId = {
        var res = ""
        val prod1 = url.split("http://www.228.com.cn/ticket-",2)
        if(prod1.length > 1)
          res = prod1.last.split(".html",2).head
        res
      }
      (agsid,(ArrayBuffer(getProductId),pv2,1))
      (agsid,(1,pv2,ArrayBuffer(getProductId)))
    }).reduceByKey((a,b)=>(a._1 + b._1,a._2 + b._2,a._3 ++ b._3))
      .filter(_._2._1 > 1)
      .map(x=>{
        val (agsid,(pv,weight,list)) = x
        val products = list.distinct.map(p=>p+"|" + dic.value.getOrElse(p,"null")).filter(!_.contains("null")).mkString(";")
        (agsid,(pv,weight,products))
      })


  }

  def tt3(): Unit ={
    /*
    http://www.228.com.cn/	Head
http://www.228.com.cn/s/	search
http://www.228.com.cn/tj/	area
http://www.228.com.cn/ticket-	product
http://www.228.com.cn/customer/reg.html 	reg
http://www.228.com.cn/help/	help
http://www.228.com.cn/shopCart/shopCart.html	shopcart
http://www.228.com.cn/integral/	integral
http://www.228.com.cn/subject/	subject
http://www.228.com.cn/common/ticket.html	ticket
http://www.228.com.cn/news/	new
http://www.228.com.cn/venue/	venue
http://login.228.com.cn/login	login
http://www.228.com.cn/cart/	cart
http://www.228.com.cn/pay/	pay
http://www.228.com.cn/security security
*/


    import com.larry.da.util.{LogParseUtil => U}
    val log = U.pvRdd(sc,"pv","2016-01-20").filter(_.contains(".228.com.cn"))

//    val allPv = log.count()
    val pvFields = "agsid,url".split(",")
    val data = log.map(U.pvLog(_)).map(d=>{
      val Array(agsid,url) = pvFields.map(f => {val v = d.getOrElse(f,"null"); if (v== "") "null" else v})

      val urlType = if(url == "http://www.228.com.cn/" ) "head"
      else if (url.startsWith("http://www.228.com.cn/s/")) "search"
      else if (url.startsWith("http://www.228.com.cn/ticket-")) "product"
      else if ( url.length - "http://www.228.com.cn/".length < 4 ) "area"
      else if (url.startsWith("http://www.228.com.cn/customer/reg.html")) "reg"
      else if (url.startsWith("http://www.228.com.cn/help")) "help"
      else if (url.startsWith("http://www.228.com.cn/shopCart")) "shopcart"
      else if (url.startsWith("http://www.228.com.cn/integral")) "integral"
      else if (url.startsWith("http://www.228.com.cn/subject")) "subject"
      else if (url.startsWith("http://www.228.com.cn/subject")) "subject"
      else if (url.startsWith("http://www.228.com.cn/common/ticket.html")) "ticket"
      else if (url.startsWith("http://www.228.com.cn/news")) "news"
      else if (url.startsWith("http://www.228.com.cn/venue")) "venue"
      else if (url.startsWith("http://login.228.com.cn/login")) "login"
      else if (url.startsWith("http://www.228.com.cn/cart")) "cart"
      else if (url.startsWith("http://www.228.com.cn/pay")) "pay"
      else if (url.startsWith("http://www.228.com.cn/security")) "security"
      else if (url.startsWith("http://www.228.com.cn/personorders/myorder.html")) "order"
      else if (url.startsWith("http://citiccard.228.com.cn")) "citiccard"
      else "unkown"
      (agsid,urlType)
    }).filter(x=>x._1 != "null" && x._2 != "null")

//  val allUv = data.map(_._1).distinct().count

    val res = data.aggregateByKey(new HashMap[String,Int])(
      (a,b) =>{
       val num = if(a.contains(b)) a(b) +1 else 1
        a +=(b -> num)
      },
      (a,b) => {
        b.foreach(e=>{
          val(k,v) =e
          val num = if(a.contains(k)) a(k) +1 else 1
          a += (k -> num)
        })
        a
      }
    ).map(x=>{
      val(agsid,d) =x ;
      val v = d.toArray.map(x=>{val(k,v)=x;k+":"+v}).sortWith((a,b)=>a<b).mkString(" ")
      (agsid,v)
    })

    res.map(x=>Array(x._1,x._2).mkString("\t")).saveAsTextFile("228pv")

    val cache = sc.textFile("228pv").cache()

  }

}
