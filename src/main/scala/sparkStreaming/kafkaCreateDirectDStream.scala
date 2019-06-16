package sparkStreaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object kafkaCreateDirectDStream {
  def main(args: Array[String]): Unit = {
    //1 初始化操作
    val sparkConf = new SparkConf().setAppName("kafkaCreateDirectDStream").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))

    //kafka参数配置
    val kafkaParams = Map("metadata.broker.list" -> "node1:9092,node2:9092,node3:9092", "group.id" -> "Kafka_Direct")

    //配置topic参数
    val topics = Set("itcast")

    //2 获取kafka中的数据
    val stream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)

    //获取InputStream中的值
    val lines: DStream[String] = stream.map(_._2)

    //具体业务
    val result: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    //执行outPut operations
    result.print()

    //开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}
