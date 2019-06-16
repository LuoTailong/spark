package rdd

import org.apache.spark.{SparkConf, SparkContext}

object PV {
  def main(args: Array[String]): Unit = {
    //需求:利用spark分析点击流日志数据---PV总量
    //1 创建SparkConf对象
    val sparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")

    //2 创建SparkContext对象
    val sc = new SparkContext(sparkConf)

    //3 读取数据文件
    val data = sc.textFile("C:\\hadoop\\test\\access.log")

    //4 获取PV总量
    println(data.count())

    //5 关闭sparkContext
    sc.stop()
  }
}
