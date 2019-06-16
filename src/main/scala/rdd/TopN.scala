package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TopN {
  def main(args: Array[String]): Unit = {
    //1 创建SparkConf对象
    val sparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")

    //2 创建SparkContext对象
    val sc = new SparkContext(sparkConf)

    //3 读取数据文件
    val data = sc.textFile("C:\\hadoop\\test\\access.log")

    //4 切分每一行,获取IP,把每个IP记为1
    val IPAndOne = data.map(x => (x.split(" ")(0), 1))

    //5 聚合操作
    val result = IPAndOne.reduceByKey(_ + _)

    //6 排序--倒序
    val sortData: RDD[(String, Int)] = result.sortBy(_._2,false)

    //7 取出前5个
    val finalResult: Array[(String, Int)] = sortData.take(5)

    //8 打印
    println(finalResult.toBuffer)//sortData.collect.foreach(x => println(x)) 或 print(sortData.collect.toBuffer)

    //9 关闭sparkContext
    sc.stop()
  }
}
