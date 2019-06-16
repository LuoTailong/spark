package sparkStreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求：通过sparkStreaming来拉取kafka中的数据---进行业务处理--进行单词计数
  */
object KafkaCreateDStream {
  def main(args: Array[String]): Unit = {
    //1 初始化操作
    val sparkConf = new SparkConf().setAppName("KafkaCreateDStream")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")//开启WAL预写日志
      .setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))

    //只要开启了writeAheadLog 就需要设置chkPoint目录
    ssc.checkpoint("./sparkStreamingCK")

    //zk config para
    val zkQuorum = "node1:2181,node2:2181,node3:2181"
    //kafka group name
    val groupId = "group-itcast"

    //指定了topic名字
    val topics = Map("itcast" -> 2)

    //2 拉取数据
    val stream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics)

    //读取kafka中的数据：数据存储在ReceiverInputDStream[(String, String)]的第二位
    val lines: DStream[String] = stream.map(_._2)

    //进行具体的业务逻辑
    val result: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    //打印计算结果
    result.print()

    //启动流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}
