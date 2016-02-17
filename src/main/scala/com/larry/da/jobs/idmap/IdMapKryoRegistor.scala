package com.larry.da.jobs.idmap

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

/**
  * Created by larry on 8/1/16.
  */
class IdMapKryoRegistor extends KryoRegistrator {
  def registerClasses(kryo: Kryo): Unit = {
    kryo.register( classOf[com.larry.da.jobs.idmap.Person])
    kryo.register( classOf[com.larry.da.jobs.userdigest.UserMapping] )//userdigest
    kryo.register( classOf[scala.collection.mutable.WrappedArray.ofRef[_]] )
  }
}
