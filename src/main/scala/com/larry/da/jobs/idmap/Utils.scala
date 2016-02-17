package com.larry.da.jobs.idmap

import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.io.{NullWritable, BytesWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer

import scala.reflect.ClassTag

/**
  * Created by larry on 7/12/15.
  */
object Utils {

  val keys="0123456789ABCDEFHIJKMNPRSTUVWXYZ"
  val keyDic = Map( keys zip (0 until 32 ) toSeq :_* )

  def compressAguid(uid:Long)={
    var n = uid
    val res = 0 until 13 map(i=>{ val index = n & 31; n = n >>> 5; keys(index.toInt)})
    res.mkString("").reverse + "u"
  }

  def unCompressAguid(uid:String)={
    val res = uid.take(13).map(s=>keyDic(s))
    var n = res.head.toLong
    res.tail.foreach(p=> {
      n = (n << 5) | p
    })
    n
  }


  def saveAsObjectFile[T: ClassTag](rdd: RDD[T], path: String) {
    val kryoSerializer = new KryoSerializer(rdd.context.getConf)

    rdd.mapPartitions(iter => iter.grouped(10)
      .map(_.toArray))
      .map(splitArray => {
        //initializes kyro and calls your registrator class
        val kryo = kryoSerializer.newKryo()

        //convert data to bytes
        val bao = new ByteArrayOutputStream()
        val output = kryoSerializer.newKryoOutput()
        output.setOutputStream(bao)
        kryo.writeClassAndObject(output, splitArray)
        output.close()

        // We are ignoring key field of sequence file
        val byteWritable = new BytesWritable(bao.toByteArray)
        (NullWritable.get(), byteWritable)
      }).saveAsSequenceFile(path)
  }

}
