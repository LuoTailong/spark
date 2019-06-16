package wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCountLocal {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val data = sc.textFile("C:\\hadoop\\test\\input\\inputword\\1.txt")
    val words = data.flatMap(_.split(" "))
    val wordAndOne = words.map((_,1))
    val result = wordAndOne.reduceByKey(_+_)
    val sortResult = result.sortBy(_._2,false)
    val finalResult = sortResult.collect()
    finalResult.foreach(x=>println(x))
    sc.stop()
  }
}

object Test {
  def main(args:Array[String]):Unit = {
    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val lst = List(1,2,3,4,5);
    print("foreach遍历：")
    lst.foreach { x => print(x+",")}  //foreach遍历,这个是传统遍历，新手不熟无奈之下可以用它
    println("")

    val rdd = sc.parallelize(List(1,2,3,5,8))
    var counter = 10
    //warn: don't do this
    val unit: Unit = rdd.foreach(x => counter += x)
    println("Counter value: "+counter)

    sc.stop()
  }
}
