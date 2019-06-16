package rdd

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpLocation {

  def ipToLong(ip: String): Long = {
    val split: Array[String] = ip.split("\\.")
    var ipNum = 0L
    for (i <- split) {
      ipNum = i.toLong | ipNum << 8L
    }
    ipNum
  }

  def binarySearch(ip: Long, valueArr: Array[(String, String, String, String)]): Int = {

    var start = 0
    var end = valueArr.length - 1

    while (start <= end) {
      var middle = (start + end) / 2

      if (ip >= valueArr(middle)._1.toLong && ip <= valueArr(middle)._2.toLong) {
        return middle
      }

      if (ip < valueArr(middle)._1.toLong) {
        end = middle
      }

      if (ip > valueArr(middle)._2.toLong) {
        start = middle
      }
    }
    -1
  }


  def data2Mysql(iterator: Iterator[(String, String, Int)]): Unit = {

    var conn: Connection = null

    var ps: PreparedStatement = null

    var sql = "insert into iplocation(longitude,latitude,total_count) values(?,?,?)"

    conn = DriverManager.getConnection("jdbc:mysql://node1:3306/itcast", "root", "hadoop")

    try {
      iterator.foreach(line => {

        ps = conn.prepareStatement(sql)

        ps.setString(1, line._1)
        ps.setString(2, line._2)
        ps.setLong(3, line._3)

        ps.execute()
      })
    } catch {
      case e: Exception => println(e)
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("IpLocation").setMaster("local[8]")

    val sc: SparkContext = new SparkContext(sparkConf)

    sc.setLogLevel("WARN")

    val ips: RDD[String] = sc.textFile("C:\\hadoop\\test\\ip.txt")

    val map: RDD[(String, String, String, String)] = ips.map(_.split("\\|")).map(x => (x(2), x(3), x(13), x(14)))

    val collect: Array[(String, String, String, String)] = map.collect()

    val broadcast: Broadcast[Array[(String, String, String, String)]] = sc.broadcast(collect)

    val destData: RDD[String] = sc.textFile("C:\\hadoop\\test\\20090121000132.394251.http.format")

    val map1: RDD[Array[String]] = destData.map(_.split("\\|"))

    val ipAddr: RDD[String] = map1.map(x => x(1))

    val partitions: RDD[((String, String), Int)] = ipAddr.mapPartitions(inter => {
      val valueArr: Array[(String, String, String, String)] = broadcast.value
      inter.map(ip => {
        val ipNum: Long = ipToLong(ip)
        val index: Int = binarySearch(ipNum, valueArr)
        val arr: (String, String, String, String) = valueArr(index)
        ((arr._3, arr._4), 1)
      })
    })
    val key: RDD[((String, String), Int)] = partitions.reduceByKey(_ + _)

    key.foreach(print)

    key.map(x => (x._1._1, x._1._2, x._2)).foreachPartition(data2Mysql)

    sc.stop()


  }
}
