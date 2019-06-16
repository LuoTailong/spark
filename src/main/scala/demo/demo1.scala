package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object demo1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("demo1").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //1 通过已经存在的集合进行创建
    val rdd1: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6,7))

    //2 由外部存储系统的文件创建
    val rdd2: RDD[String] = sc.textFile("C:\\hadoop\\test\\input\\inputword\\1.txt")

    //3 已有的RDD经过算子转换成新的RDD
    val rdd3: RDD[String] = rdd2.flatMap(_.split(" "))

    val rdd4 = rdd3.map((_,1))

    val count = rdd3.count()
    val first = rdd3.first()
    val take = rdd3.take(10)
    val sorted = rdd4.sortByKey(false)

    rdd1.foreach(x=>print(x))
    rdd3.foreach(x=>println(x))
  }
}
