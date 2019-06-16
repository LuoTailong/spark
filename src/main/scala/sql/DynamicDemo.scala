package sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DynamicDemo {
  def main(args: Array[String]): Unit = {
    //初始化操作
    val spark: SparkSession = SparkSession.builder().appName("DynamicDemo").master("local[2]").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    sc.setLogLevel("WARN")

    //加载数据
    val lines: RDD[String] = sc.textFile("C:\\hadoop\\test\\Person.txt")

    //切分数据
    val words: RDD[Array[String]] = lines.map(_.split(" "))

    //将数据转换成RDD[Row]
    val rowRDD: RDD[Row] = words.map(x => Row(x(0).toInt, x(1), x(2).toInt))

    val schema = StructType(StructField("id", IntegerType, false) :: StructField("name", StringType, false) :: StructField("age", IntegerType, false) :: Nil)

    ///创建dataframe
    val dataFrame = spark.createDataFrame(rowRDD,schema)
    dataFrame.show()
    dataFrame.printSchema()

    //释放资源
    sc.stop()
    spark.stop()
  }
}
