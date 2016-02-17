package com.larry.da.util

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable.ArrayBuffer

/**
 * Created by larry on 3/20/15.
 */
object StreamingKafkaUtil {

  def createKafkaStream(ssc:StreamingContext, topics:String, groupId:String, recieverNum:Int = 1):DStream[String] = {
    val zkQuorum = "l-kafka2.ucld.vps.corp.agrant.cn:2181,l-kafka3.ucld.vps.corp.agrant.cn:2181,l-kafka1.ucld.vps.corp.agrant.cn:2181"
    val topicMap = topics.split(",").map((_,1)).toMap
    
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum, 
      "group.id" -> groupId,
      "zookeeper.connection.timeout.ms" -> "10000",
      "rebalance.max.retries" -> "150")
    
    //receiving data parallel
    val kafkaStreamList =(1 to recieverNum).map(
//        i=> KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap) 
        i => KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2)
    )
    val kafkaStream = ssc.union(kafkaStreamList).map(_._2)
    kafkaStream
  }

  def getKafkaStream(ssc:StreamingContext, topics:String, groupId:String, recieverNum:Int = 1):DStream[String] = {
    val zkQuorum = "l-kafka2.ucld.vps.corp.agrant.cn:2181,l-kafka3.ucld.vps.corp.agrant.cn:2181,l-kafka1.ucld.vps.corp.agrant.cn:2181"
    val topicMap = topics.split(",").map((_,1)).toMap

    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> groupId,
      "zookeeper.connection.timeout.ms" -> "10000",
      "rebalance.max.retries" -> "150")

    //receiving data parallel
    val kafkaStreamList =(1 to recieverNum).map(
      //        i=> KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap)
      i => KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2)
    )
    val kafkaStream = ssc.union(kafkaStreamList).map(_._2)
    kafkaStream
  }

  def createKafkaProducer() ={
    val props = new Properties()
    val brokers="l-kafka2.ucld.vps.corp.agrant.cn:9092,l-kafka3.ucld.vps.corp.agrant.cn:9092,l-kafka1.ucld.vps.corp.agrant.cn:9092"
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "async")
    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](kafkaConfig)
    producer
  }



  def getKafkaProducer() ={
    val props = new Properties()
    val brokers="l-kafka2.ucld.vps.corp.agrant.cn:9092,l-kafka3.ucld.vps.corp.agrant.cn:9092,l-kafka1.ucld.vps.corp.agrant.cn:9092"
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "async")
    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](kafkaConfig)
    producer
  }



  def collectAsBatch(arrBuffer:ArrayBuffer[ArrayBuffer[KeyedMessage[String,String]]],message:String,topic:String,batchSize:Int)={
    val keyedMessage = new KeyedMessage[String, String](topic, message);
    if(arrBuffer.last.length < batchSize){arrBuffer.last += keyedMessage;arrBuffer;}
    else arrBuffer += {new ArrayBuffer[KeyedMessage[String, String]](batchSize) += keyedMessage}
  }

  import java.security.MessageDigest;
  val hexDigits = Array( '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' );
  def md5(data:String)={
    MessageDigest.getInstance("MD5").digest(data.getBytes).flatMap(x=>{
      Array( hexDigits( x >>> 4 & 0xf),  hexDigits( x & 0xf) )
    }).mkString
  }

}