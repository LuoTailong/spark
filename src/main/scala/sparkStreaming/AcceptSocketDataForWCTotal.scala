package sparkStreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求：接收socket数据 进行全局汇总的单词计数
  */
object AcceptSocketDataForWCTotal {
  def updateFunc(newValue: Seq[Int], runningValue: Option[Int]): Option[Int] = {

    val result: Int = newValue.sum + runningValue.getOrElse(0)

    Some(result)
  }

  def updateFunc1(currentV: Seq[Int], historyV: Option[Int]): Option[Int] = {

    val result: Int = currentV.sum +historyV.getOrElse(0)

    Some(result)
  }

  def main(args: Array[String]): Unit = {
    //1 创建一个sparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("AcceptSocketDataForWCTotal").setMaster("local[4]")

    //2 创建sparkContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //3 创建一个streamingContext对象
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(5))

    //需要记录历史数据进行统计 就要把历史数据保存下来 必须要指定一个路径
    ssc.checkpoint("./ck")

    //4 接收socket的数据 通过socketTextStream来获取数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node1",9999)

    //5 单词计数
    val result: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    val historyResult: DStream[(String, Int)] = result.updateStateByKey(updateFunc)

    //6 对计算的结果进行输出
    historyResult.print()

    //启动流式计算
    ssc.start()

    //让程序一直进行
    ssc.awaitTermination()
  }
}
