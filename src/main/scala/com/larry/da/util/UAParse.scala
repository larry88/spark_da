package com.larry.da.util

/**
 * Created by larry on 29/9/15.
 */


object UAParse {
  val dicKeyword = Map(
    "TencentTraveler"   ->"brw",
    "QQBrowser" ->"brw",
    "Maxthon"   ->"brw",
    "BIDUBrowser"   ->"brw",
    "360SE" ->"brw",
    "TheWorld"  ->"brw",
    "qihu theworld" ->"brw",
    "UCBrowser" ->"brw",
    "Firefox"   ->"brw",
    "Chrome"    ->"brw",
    "MSIE"  ->"brw",
    "MQQBrowser"    ->"brw",
    "NokiaBrowser"  ->"brw",
    "Iceweasel" ->"brw",
    "K-MeleonCCFME" ->"brw",
    "Opera" ->"brw",
    "BdMobile"  ->"brw",
    "UC"    ->"brw",
    "Safari"    ->"brw",
    "Mobile"    ->"brw",
    "UCWEB" ->"brw",
    "SE 2.X"    ->"brw",
    "Trident" -> "brw",
    "spider" -> "brw",

    "AppleWebKit"   ->"knl",
    "Presto"    ->"knl",
    "Gecko" ->"knl",
    "KHTML" ->"knl",

    "SymbianOS" ->"os",
    "Mac OS X"  ->"os",
    "Android"   ->"os",
    "Windows NT"    ->"os",
    "Adr"   ->"os",
    "Linux" ->"os"
    )

  val window_nt = Map(
    "5.0" -> "Windows 2000",
    "5.1" -> "Windows XP",
    "5.2" -> "Windows 2003",
    "6.0" -> "Windows Vista",
    "6.1" -> "Windows 7",
    "6.3" -> "Windows 8"
  )

  val regAdroidHard1 = """([\w -]+)( Build/)""".r
  val regAdroidHard2 = """; ([\w -]+)\)""".r
  val regAdroidHard3 = """(^[\w-]+)""".r
  val regMacHard = """\( *([a-zA-Z]+) *;""".r
  val regMacVersion = """OS ([0-9_]+)""".r
  val regLinuxVersion = """(Linux) ([\w-]+)""".r
  val regHardCheck = """^[\d\- _]+$""".r
  val regIEVersion = """rv:(\d+)""".r

  val androidHard = Array("browser","mozilla","opera","ucweb")

  def getInfo(ua:String)={
    val pattern = dicKeyword.keys.mkString("(","|",")") + "([0-9/. ]*)";
    val cpat = s"""$pattern""".r
//    val ua = "LENOVO-Lenovo-A298t/1.0 Linux/2.6.35.7 Android 2.3.5 Release/11.22.2012 Browser/AppleWebKit533.1 (KHTML, like Gecko) Mozilla/5.0 Mobile"
    var(brw,brwv,knl,knlv,os,osv,hard)=("","","","","","","")
    cpat.findAllMatchIn(ua).foreach(x=>{
      val groupCount = x.groupCount;
      val key = x.group(1)
      if( dicKeyword.contains(key)){
        dicKeyword(key) match {
          case "brw" =>{
            if(brw == "Safari" || brw == ""){
              brw=key;
              brwv=x.group(2);
            }
            if(brw == "Trident"){
              brw = "MSIE";
              regIEVersion.findFirstMatchIn(ua).foreach(m=>brwv=m.group(1))
            }
          }
          case "knl" =>{knl=key;knlv=x.group(2)}
          case "os" =>{
            os=key;
            osv=x.group(2)
            if(os == "Adr") os = "Android"
            if(os == "Android" ){
              regAdroidHard1.findFirstMatchIn(ua).foreach(m=>hard=m.group(1))
              if(hard == ""){
                val start = ua.lastIndexOf(";")
                if(start >=0) regAdroidHard2.findFirstIn(ua.substring(start)).foreach(hard = _)
              }
              if(hard == "")
                regAdroidHard3.findAllMatchIn(ua).foreach(m=>{
                  val h = m.group(0).toLowerCase
                  if( androidHard.forall(h.indexOf(_) >= 0 ) )
                    hard = m.group(0)
                })
            }else if (os == "Mac OS X"){
              regMacHard.findFirstIn(ua.substring(x.start(1))).foreach(hard = _)
              regMacVersion.findFirstIn(ua.substring(x.start(1))).foreach(osv = _)
            }else if(os == "Linux"){
              regLinuxVersion.findFirstMatchIn(ua.substring(x.start(1))).foreach(m=> osv=m.group(2))
            }
          }
        }
      }
    })

    if(brwv.length > 0) {  val splits = brwv.replace(" ","").replace("/","").split("\\."); brwv = if(splits.length > 0)splits.head else "" }
    osv= osv.replace(" ","").replace("/","")
    knlv= knlv.replace(" ","").replace("/","")

    if(os == "Windows NT") if(window_nt.contains(osv)) os = window_nt(osv);
    if(os == "Android" && osv != "") osv =osv.split("\\.").take(2).mkString(".")

    hard = hard.trim
    regHardCheck.findFirstIn(hard).foreach(v=>hard="")

    (brw,brwv,knl,knlv,os,osv,hard)

  }


  def main(args: Array[String]): Unit = {
    val list = Array(
//      "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.12 (KHTML, like Gecko) Maxthon/3.4.1.1000 Chrome/18.0.966.0 Safari/535.12"
//    ,"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 708; BTRS4136)"
//    ,"MQQBrowser/3.7/Mozilla/5.0 (Linux; U; Android 2.3.5; zh-cn; GT-N7000 Build/GINGERBREAD) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1"
//    ,"MQQBrowser/2.7 Mozilla/5.0 (iPad; U; CPU OS 4_3_5 like Mac OS X; zh-cn) AppleWebKit/533.17.9 (KHTML, like Gecko) Mobile/8L1 Safari/7534.48.3"
//    ,"Mozilla/5.0 (Linux; U; Android 4.1.2; zh-CN; GT-N7108 Build/JZO54K) AppleWebKit/534.31 (KHTML, like Gecko) UCBrowser/9.3.2.349 U3/0.8.0 Mobile Safari/534.31"
//    ,"Mozilla/5.0 (Linux; U; Android 2.3.6; zh-cn; YLT-E603 Build/GRK39F) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1 baiduboxapp/4.7.1 (Baidu; P1 2.3.6)"
//    ,"UCWEB/2.0 (Linux; U; Adr 2.3.6; zh-CN; epade A360S) U2/1.0.0 UCBrowser/9.3.0.321 U2/1.0.0 Mobile"
//    ,"UCWEB/2.0 (Linux; U; Adr 4.1.1; zh-CN; MI 2) U2/1.0.0 UCBrowser/9.3.0.321 U2/1.0.0 Mobile"
//    ,"LENOVO-Lenovo-A298t/1.0 Linux/2.6.35.7 Android 2.3.5 Release/11.22.2012 Browser/AppleWebKit533.1 (KHTML, like Gecko) Mozilla/5.0 Mobile"
//    ,"Mozilla/5.0 (iPod; CPU iPhone OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Mobile/9B206)"
//    ,"Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko)"
    "Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)"
    )

    list.map(getInfo(_)).foreach(println)

  }

}
