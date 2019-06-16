package wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //1 创建sparkConfig对象 设置appName和master的地址
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount")

    //2 创建sparkContext对象
    val sc = new SparkContext(sparkConf)

    //设置日志输出级别
    sc.setLogLevel("WARN")

    //3 读取数据文件
    val data: RDD[String] = sc.textFile(args(0))

    //4 切分文件中的每一行 返回文件所有的单词
    val words: RDD[String] = data.flatMap(_.split(" "))

    //5 每个单词记为1
    val wordAndOne: RDD[(String, Int)] = words.map((_,1))

    //6 相同单词出现的次数累加
    val result: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)

    //按照单词出现的次数降序排列
    val sortResult: RDD[(String, Int)] = result.sortBy(_._2,false)

    //7 按照数据保存在HDFS上
    sortResult.saveAsTextFile(args(1))

    //8 关闭sc
    sc.stop()
  }
}
