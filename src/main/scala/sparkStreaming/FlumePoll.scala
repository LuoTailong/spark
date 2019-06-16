package sparkStreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求：通过sparkStreaming来拉取flume中的数据，然后进行单词计数
  *
  * 难点：怎么样拉取函数入口？
  * FlumeUtils
  *
  * 特点：
  * 1、sparkStreaming程序主动的去flume中拉取数据
  * 2、
  */
object FlumePoll {
  def main(args: Array[String]): Unit = {
    //1 初始化工作
    val sparkConf = new SparkConf().setAppName("FlumePoll").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //2 创建StreamingContext
    val ssc = new StreamingContext(sc,Seconds(5))

    //3 通过sparkStreaming和flume的整合包中有一个入口FlumeUtils
    val stream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc,"node1",8888)

    //4 将具体的数据拿出来 数据在body中
    val lines: DStream[String] = stream.map(x=>new String(x.event.getBody.array()))

    //5 进行单词计数
    val result: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    //6 输出结果
    result.print()

    //7 启动程序
    ssc.start()
    ssc.awaitTermination()
  }
}
