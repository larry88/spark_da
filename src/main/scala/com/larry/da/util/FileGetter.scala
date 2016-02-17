package com.larry.da.util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Created by larry on 5/5/15.
 */
class FileGetter(sc:SparkContext,dir:String,patternPrefix:String){
  val REGEX = (patternPrefix + """([\d-]+)*$""").r
  var fs_ : FileSystem = _


  def fs = synchronized {
    if (fs_ == null) fs_ = new Path(dir).getFileSystem(sc.hadoopConfiguration)
    fs_
  }

  def sortFunc(path1: Path, path2: Path): Boolean = {
    val time1= path1.getName match { case REGEX(x) => x }
    val time2= path2.getName match { case REGEX(x) => x }
    (time1 > time2)
  }

  def getFileList()={
    val path = new Path(dir)
    val files = fs.listStatus(path).map(_.getPath).filter(p => REGEX.findFirstIn(p.toString).nonEmpty).sortWith(sortFunc)
    files
  }

  def getNewestFile() ={
    getFileList()(0).toString
  }

  def feedQueue(rddQueue:mutable.SynchronizedQueue[RDD[String]],secondSpan:Int): Unit ={
    var lastFileName = getNewestFile();
    var curFileName = ""
    var lastConfigRdd = sc.textFile( lastFileName ).cache()
    while(true) {
      try {
        curFileName = getNewestFile()
        //if has new file,then update rdd
        if (curFileName != lastFileName) {
          val configRdd = sc.textFile(lastFileName).cache()
          val rowCount = configRdd.count()
          if (rowCount > 10) {
            println(s"config name : $lastFileName count is : $rowCount")
            lastConfigRdd = configRdd
            lastFileName = curFileName
          }
        }
      } catch {
        case e: Exception => {
          println(s"******has err: $e , \nuse lastConfigRdd :$lastFileName")
        }
      };

      //be sure the queue has 3 rdd,because sleep is not timer,sleeptime + runtime > sparkStreamingTimer
      while (rddQueue.size < 3) {
        rddQueue += lastConfigRdd;
      }
      println("queue size is : " + rddQueue.size)

      Thread.sleep((secondSpan * 1000).toLong)
    }
  }
}

