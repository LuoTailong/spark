package sparkStreaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求：通过reduceByKeyAndWindow来实现单词技术
  * 两个参数需要注意：
  *   1、窗口大小
  *   2、滑动的时间
  *
  *   如果参数没有设计好，会出现数据的重复计算和数据丢失
  */
object SparkStreamingWindowFunc {
  def main(args: Array[String]): Unit = {
    //1 初始化操作
    val sparkConf = new SparkConf().setAppName("SparkStreamingWindowFunc").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //2 sparkStreaming函数的入口对象
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(5))

    //3 通过StreamingContext对象来获取socket数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node1",9999)

    //4 将单次计数返回结果 两个注意的参数：窗口的长度，滑动窗口的时间  (注意：最好是创建StreamingContext批次时间的整数倍)
    val window = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(10), Seconds(10))
    //val window = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(5), Seconds(5))正常
    //val window = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(5), Seconds(10))丢数据
    //val window = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(10), Seconds(5))重复计算数据

    //output opera
    window.print()

    //启动程序
    ssc.start()
    ssc.awaitTermination()
  }

}
