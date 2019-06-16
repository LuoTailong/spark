package rdd

object Test {
  def main(args: Array[String]): Unit = {
    val ip="1.3.0.0"
    println(ip2Long((ip)))
  }

  //将ip地址转化为Long   192.168.200.100
  def ip2Long(ip: String): Long = {
    //将IP地址转为Long,这里有固定的算法
    val ips: Array[String] = ip.split("\\.")
    var ipNum:Long=0L
    for(i <- ips){
      ipNum=i.toLong | ipNum <<8L
    }
    ipNum
  }
}
