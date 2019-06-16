package sql

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object JDBCDemo {
  def main(args: Array[String]): Unit = {
    //初始化操作
    val spark = SparkSession.builder().appName("JDBCDemo").master("local[2]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //配置登陆数据库的名字 密码
    val properties: Properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","hadoop")

    //通过spark.read.jdbc来读取数据库的信息 返回结果为DataFrame
    val dataframe: DataFrame = spark.read.jdbc("jdbc:mysql://node1:3306/itcast","iplocation",properties)
    dataframe.show()
    dataframe.printSchema()

    //写入的数据是什么 write.jdbc
    //dataframe.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://node1:3306/itcast","iplocation111",properties)
    dataframe.write.mode(SaveMode.Append).jdbc("jdbc:mysql://node1:3306/itcast","iplocation111",properties)

    sc.stop()
    spark.stop()
  }
}
