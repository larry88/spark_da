package com.larry.da.parse

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.rdd.RDD
import com.google.common.hash.Hashing.md5
import scala.collection.mutable.ArrayBuffer



/**
 * Created by larry on 16/11/15.
 */
object SiteClass {

  var sc : SparkContext = _

  def binaryRelation(day:String): Unit ={

    import scala.collection.mutable.ArrayBuffer
    import com.larry.da.util.{LogParseUtil => U}
    val log = sc.union(
      day.split(",").flatMap(hour=>{
        "show".split(",").flatMap(logType => {
          "baidu,adx,tanx".split(",").map(ch=> U.dspRdd(sc,logType,ch,hour))
        })
      })
    )
    val fields = "guid,site".split(",")
    val data1 = log.map(x=>{
      val d = U.showLog(x)
      val Array(guid,site) = fields.map(f => {val fValue = d.getOrElse(f,"null");if(fValue == "") "null" else fValue})
      (guid,site)
    }).filter(x=>x._1 != "null" && x._2 != "null").distinct(sc.defaultParallelism * 3)
    val uidContent = data1.map(x=>{
      val(guid,site) = x;
      (guid,ArrayBuffer(site))
    }).reduceByKey(_++=_)
    val data2 =  uidContent.filter(x=>{val len = x._2.length; len > 1 && len < 25}).flatMap(x=>{
      val(guid,list) = x;
      val res = for(s1<-list;s2<-list) yield(s1,s2)
      res.filter(p=>p._1 < p._2).map(p=>(p,1))
    }).reduceByKey(_+_)
    data2.map(x=>{
      val((s1,s2),num) = x;
      Array(s1,s2,num).mkString("\t")
    }).saveAsTextFile(s"show/$day")

  }

  def run1(): Unit ={

    7 to 14 foreach(d =>{
      val day = if(d < 10) "0" + d else d.toString
      val date = "2015-11-" + day
      println (date)
      binaryRelation(date)
    })

  }





  //========== vertices and edges =================
  def prepareGragh(data: RDD[((String, String), Double)]) = {
    val res = data.flatMap(x => {
      val ((s1, s2), num) = x;
      val (s1L, s2L) = (md5.hashString(s1).asLong(), md5.hashString(s2).asLong())
      Array(
        (1, (s1L,s1,0)),
        (1, (s2L,s2,0)),
        (2, (s1L,s2L.toString,num))
      )
    })

    val vertices = res.filter(_._1 == 1).map(x => {
      val(xxk,(siteL,site,xx3)) = x;
      (siteL,site)
    }).distinct()


    val edges = res.filter(_._1 == 2).map(x => {
      val(xxk,(s1L,s2,num)) = x;
      (s1L,s2.toLong,num)
    })
    (vertices, edges)
  }



  def processGraph(): Unit ={

    val log = sc.textFile(s"show/*")
      .repartition(sc.defaultParallelism * 3)
      .map(x=>x.split("\t")).filter(x=>x.length == 3).map(x=>{val Array(s1,s2,num) = x;((s1,s2),num.toInt)})

    val data = log.reduceByKey(_+_).filter(_._2 > 30).flatMap(x=>{
      val((s1,s2),num) = x;
      Array(
        (s1,(s2,num)),
        (s2,(s1,num))
      )
     }).aggregateByKey(new ArrayBuffer[(String,Int)])(
      (a,v) => a += v,
      (a,b) => a ++= b
    ).filter(_._2.length > 4).cache()

    val verticelDic = sc.broadcast( data.map(x=>{
        val(s1,list) =x;
        var totalWeight = 0;
        val edgeCount = list.length
        list.foreach( p=> totalWeight += p._2 )
        (s1,(totalWeight,edgeCount))
      }).collectAsMap()
    )

    val sortedRdd = data.map(x=>{
      val(site,list) =x;
      val sorted = list.map(p=>{
        val(s,num) = p;
        val (totalWeight,edgeCount) = verticelDic.value.getOrElse(s,(Int.MaxValue,Int.MaxValue))
        val w = (num.toDouble / totalWeight) * math.log( Int.MaxValue.toDouble / edgeCount )
        (s,w)
      }).sortWith((a,b) => a._2 > b._2)
      (site,sorted)
    })

    val resData = sortedRdd.flatMap(x=>{
      val(site,list) = x;
      list.take(1).filter(p=>p._2 > 1D).map(p=>{
        ((site,p._1),p._2)
      })
    })




    val(vertices,edges) = prepareGragh( resData )

    val edgeRdd = edges.map(x=>{
      val (s1,s2,num) =x ;
      Edge(s1,s2,null)
    }).repartition(sc.defaultParallelism)

    val res = Graph.fromEdges(edgeRdd,null).connectedComponents().vertices
//    res.map(_._2).distinct().count()
//    siteDelDic.value.count(x=>true)
    res.map(x=>(x._2,ArrayBuffer(x._1))).reduceByKey(_++=_).map(x=>(x._2.length,x._1)).sortByKey(false).take(50).foreach(println)

    res.join(vertices).map(x=>{
      val (sL,(cL,site)) =x;
      (cL,ArrayBuffer(site))
    }).reduceByKey(_++=_).map(x=>x._2.mkString(",")).saveAsTextFile("siteClass/res")

    /*
    (72,-8925578992189951322)
(66,-9171405986732911112)
(54,-9104471626576158611)
(48,-8863894869090580160)
(47,-9125154861068097076)
(41,-8903496469600426327)
(38,-9087393912466866681)
(32,-8203124030491385269)
(32,-8841686522496213865)
(30,-9147281699495493982)
(25,-8722281529839030491)
(25,-9059556491083049789)
(25,-9024733272072633538)
(24,-7233829474210095194)
(21,-8203113167436391916)
(21,-6643023523241620483)
(20,-8995308617529765229)
(19,-8958346478855326031)
(18,-8926357782926982346)
(17,-8626248322765934824)
(17,-7508195520869170376)
(16,-9135162308908727993)
(15,-8649697749656082988)
(15,-8551153477254068310)
(15,-8817568393341188252)


*/
    res.filter(_._2 == -8925578992189951322L).join(vertices).take(100).foreach(println)
    res.filter(_._2 == -5027747965319986579L).join(vertices).take(100).foreach(println)
  }


  def test1(): Unit ={

    val log = sc.textFile(s"show/*")
      .repartition(sc.defaultParallelism * 3)
      .filter(! _.contains("www.anonymouswebsite.com")) //delete special website
      .map(x=>x.split("\t")).filter(x=>x.length == 3).map(x=>{val Array(s1,s2,num) = x;((s1,s2),num.toInt)})

    val t1 = log.reduceByKey(_+_).flatMap(x=>{
      val((s1,s2),num) = x;
      Array(
        (s1,ArrayBuffer((s2,num))),
        (s2,ArrayBuffer((s1,num)))
      )
    }).reduceByKey(_++=_)

    t1.map(x=>{
      val(site,list)=x;
      site + "," +list.map(p=>Array(p._1,p._2).mkString(":")).mkString(",")
    }).saveAsTextFile("siteClass/res2")

    val keyNum =  Array(10,20,50,100,200,500,1000,2000,Int.MaxValue)
    val kn = keyNum zip  Array("   0-9","  10-19","  20-49","  50-99"," 100-199"," 200-499"," 500-999","1000-1999","else")
    t1.map(x=>{
      val(site,list) =x;
      val len = list.length
      val k = kn.map(n=>(len/n._1,n._2)).filter(_._1 == 0).head._2
      (k,1)
    }).reduceByKey(_+_).sortByKey().map(x=>{
      val(span,num) =x;
      Array(span,num).mkString(",")
    }).take(200).foreach(println)

  }


}
