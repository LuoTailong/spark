package demo

import org.apache.spark.{SparkConf, SparkContext}

object demo2_1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("demo2").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val rdd1 = sc.parallelize(List(5, 6, 4, 7, 3, 8, 2, 9, 1, 10))
    val rdd2 = rdd1.map(_ * 2).sortBy(x => x, true)
    val rdd3 = rdd2.filter(_ >= 5)
    println(rdd3.collect().toBuffer)
  }
}

object demo2_2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("demo2").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val rdd1 = sc.parallelize(Array("a b c", "d e f", "h i j"))
    val rdd2 = rdd1.flatMap(_.split(" "))
    println(rdd2.collect().toBuffer)
  }
}

object demo2_3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("demo2").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val rdd1 = sc.parallelize(Array(5, 6, 4, 3))
    val rdd2 = sc.parallelize(Array(1, 2, 3, 4))

    //并集
    val rdd3 = rdd1.union(rdd2)
    //交集
    val rdd4 = rdd1.intersection(rdd2)
    //去重
    val rdd5 = rdd3.distinct

    println(rdd4.collect().toBuffer)
    println(rdd5.collect().toBuffer)
  }
}

object demo2_4 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("demo2").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val rdd1 = sc.parallelize(List(("tom", 1), ("jerry", 3), ("kitty", 2)))
    val rdd2 = sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2)))

    //join
    val rdd3 = rdd1.join(rdd2)
    //并集
    val rdd4 = rdd1.union(rdd2)
    //去重
    val rdd5 = rdd4.groupByKey()

    val rdd6 = rdd4.groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2))
    println(rdd6.collect().toBuffer)
  }
}

object demo2_5 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("demo2").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val rdd1 = sc.parallelize(List(("tom", 1), ("tom", 2), ("jerry", 3), ("kitty", 2)))
    val rdd2 = sc.parallelize(List(("jerry", 2), ("tom", 1), ("jim", 2)))

    //cogroup
    val rdd3 = rdd1.cogroup(rdd2)
    val rdd4 = rdd3.collect.map(x => (x._1, x._2._1.sum + x._2._2.sum))
    println(rdd4.toBuffer)
  }
}

object demo2_6 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("demo2").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5))

    //reduce
    val rdd2 = rdd1.reduce(_ + _)
    println(rdd2)
  }
}

object demo2_7 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("demo2").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val rdd1 = sc.parallelize(List(("tom", 1), ("jerry", 3), ("kitty", 2), ("shuke", 1)))
    val rdd2 = sc.parallelize(List(("jerry", 2), ("tom", 3), ("shuke", 2), ("kitty", 5)))
    val rdd3 = rdd1.union(rdd2)

    //普通解法
    val result = rdd3.groupBy(_._1).map(x=>(x._1,x._2.map(_._2).sum))
    println(result.collect.toBuffer)

    //reduce解法
    val rdd4 = rdd3.reduceByKey(_ + _)
    println(rdd4.collect.toBuffer)

    val rdd5 = rdd4.map(t => (t._2, t._1)).sortByKey(false).map(t => (t._2, t._1))
    println(rdd5.collect.toBuffer)
  }
}

object demo2_8 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("demo2").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val rdd1 = sc.parallelize(1 to 10,3)

    //利用repartition改变rdd1分区数
    //减少分区
    rdd1.repartition(2).partitions.size
    //增加分区
    rdd1.repartition(4).partitions.size

    //利用coalesce改变rdd1分区数 只能减不能加
    //减少分区
    rdd1.coalesce(2).partitions.size
  }
}
