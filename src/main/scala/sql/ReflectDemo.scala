package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReflectDemo {

  //需求：通过反射来操作DataFrame

  //创建一个样例类与数据格式相对应
  case class Person(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    //初始化操
    val spark: SparkSession = SparkSession.builder().appName("ReflectDemo").master("local[2]").getOrCreate()

    val sc = spark.sparkContext

    //过滤日志
    sc.setLogLevel("WARN")

    //加载数据
    val lines: RDD[String] = sc.textFile("C:\\hadoop\\test\\Person.txt")

    //切分数据
    val words: RDD[Array[String]] = lines.map(_.split(" "))

    //将数据与样例类进行关联
    val personRDD: RDD[Person] = words.map(x=>Person(x(0).toInt,x(1),x(2).toInt))

    //将RDD转换为DataFrame
    import spark.implicits._
    val dataframe: DataFrame = personRDD.toDF

    //DataFrame
    dataframe.show()
    dataframe.printSchema()
    dataframe.select("age","name").show()
    dataframe.filter($"age">25).show()

    //通过sql方式来写的时候，一定要注册为一张表
    dataframe.createOrReplaceTempView("person")
    spark.sql("select * from person where age >25").show()
    sc.stop()
    spark.stop()

  }

}
