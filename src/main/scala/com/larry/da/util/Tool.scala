package com.larry.da.util

import com.google.common.hash.Hashing

/**
 * Created by larry on 15-7-10.
 */
object Tool {



  import java.security.MessageDigest;
  val hexDigits = Array( '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' );
  def md5(data:String)={
    MessageDigest.getInstance("MD5").digest(data.getBytes).flatMap(x=>{
      Array( hexDigits( x >>> 4 & 0xf),  hexDigits( x & 0xf) )
    }).mkString
  }

  def tt={
    com.google.common.hash.Hashing.md5().hashString("234").asLong();
  }


}
