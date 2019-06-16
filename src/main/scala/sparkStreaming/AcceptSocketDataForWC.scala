package sparkStreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求：接收socket的数据，进行单词计数
  *
  */
object AcceptSocketDataForWC {
  def main(args: Array[String]): Unit = {
    //1 创建一个对象
    val sparkConf = new SparkConf().setAppName("AcceptSocketDataForWC").setMaster("local[4]")

    //2 创建SparkContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //3 创建一个StreamingContext对象
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(5))

    //4 接收socket的数据 通过socketTextStream来获取数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node1",9999)

    //5 单词计数
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordAnd1 = words.map((_,1))

    //一组相同key的数据进行累加
    val result = wordAnd1.reduceByKey(_+_)

    //6 对计算结果进行输出
    result.print()

    //启动流式计算
    ssc.start()

    //让程序一直执行
    ssc.awaitTermination()
  }
}
