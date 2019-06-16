package sparkStreaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求：通过开窗函数来统计一定时间间隔内的热门词汇---topN
  *
  * 关键点：需要在开窗函数的结果中，要进行排序-降序
  */
object SparkStreamingTOPN {
  def main(args: Array[String]): Unit = {
    //1：初始化操作
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingTOPN").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //2、sparkStreaming的入口
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    //3、接收数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node1", 9999)

    //4、数据转换成元组(单词，1)
    val wordAnd1: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))

    //5 使用开窗函数进行计算
    val result: DStream[(String, Int)] = wordAnd1.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(5), Seconds(5))

    //6 对当前时间范围之内的数据进行排序 取top3
    val data: DStream[(String, Int)] = result.transform(rdd => {
      val dataRDD: RDD[(String, Int)] = rdd.sortBy(_._2, false)
      val hotWords = dataRDD.take(3)
      println("-----------start-----------")

      println(hotWords.toBuffer)

      println("-----------end-----------")
      dataRDD
    })

    data.print()

    //启动流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}
