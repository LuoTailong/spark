package sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object OperaHive {
  def main(args: Array[String]): Unit = {
    //初始化操作
    val spark: SparkSession = SparkSession.builder().appName("OperaHive").master("local[2]")
      .enableHiveSupport()//操作hive必须要打开操作hive的开关
      .config("spark.sql.warehouse.dir","C:\\hadoop\\test\\spark-warehouse")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    sc.setLogLevel("WARN")

    spark.sql("create table if not exists person(id int,name string, age int) row format delimited fields terminated by ' '")
    spark.sql("load data local inpath './data/student.txt' into table person")
    spark.sql("select * from person").show()

    sc.stop()
    spark.stop()
  }
}
