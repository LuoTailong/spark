package rdd

import org.apache.spark.{SparkConf, SparkContext}

object UV {
  def main(args: Array[String]): Unit = {
    //1 创建SparkConf对象
    val sparkConf = new SparkConf().setAppName("UV").setMaster("local[2]")

    //2 创建SparkContext对象
    val sc = new SparkContext(sparkConf)

    //3 读取数据文件
    val data = sc.textFile("C:\\hadoop\\test\\access.log")

    //4 切分每一行获取ip地址
    val ips = data.map(_.split(" ")(0))

    //5 获取UV总量
    val distinctIps = ips.distinct()

    //6 获取UV总量
    println(distinctIps.count())

    //7 关闭sparkContext
    sc.stop()
  }
}
