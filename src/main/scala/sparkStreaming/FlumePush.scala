package sparkStreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object FlumePush {
  def main(args: Array[String]): Unit = {
    //1 初始化操作
    val sparkConf: SparkConf = new SparkConf().setAppName("FlumePush").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //2 创建ssc对象
    val ssc = new StreamingContext(sc,Seconds(5))

    //3 接收flume push过来的数据
    val stream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createStream(ssc,"192.168.155.58",9999)

    //4 读取数据
    val lines: DStream[String] = stream.map(x=>new String(x.event.getBody.array()))

    //5 业务逻辑处理 单词计数
    val result: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    //6 获取数据 进行打印
    result.print()

    //7 开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}
