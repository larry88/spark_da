package com.larry.da.jobs.userdigest

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by larry on 14/12/15.
  */
object UserdigestTest {




  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("channelid-merge")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.mb","128")
    //    conf.set("spark.kryo.registrationRequired", "true")
    conf.registerKryoClasses(Array(
      classOf[com.larry.da.jobs.idmap.Person],
      classOf[com.larry.da.jobs.userdigest.UserMapping],//userdigest
      classOf[scala.collection.mutable.WrappedArray.ofRef[_]]
    ))
    val sc = new SparkContext(conf)

    ChannelIdMerge.mergeIdMap(sc,"2016-01-17")

  }

}
