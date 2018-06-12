package cn.zhangjin.spark.day01

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @author zhangjin
  * @create 2018-06-12 16:59
  */
object DatasetDemo2 {

  def main(args: Array[String]): Unit = {
    //统一的API入口
    // session 或者 使用 spark  如果当前上下文环境中 有 直接拿来使用 否则 就再创建
    //指定  如何指定 local 还是 集群模式
    val session: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    //导入SparkSession上面的隐士转换
    import session.implicits._
    //如何从SparkSession中 获取到 SparkContext和sqlContext
    //    val sc: SparkContext = session.sparkContext
    //    val sqlContext: SQLContext = session.sqlContext


    //Session API获取到的对象数据类型 就是 dataset
    val file: Dataset[String] = session.read.textFile("/Users/zhangjin/myCode/learn/spark-sql/input/person.txt")

    val tpDs: Dataset[(String, Int, Int)] = file.map(_.split(" ")).map(t=>(t(0),t(1).toInt,t(2).toInt))

    tpDs.printSchema()

    /**
      * 元组类型的dataset的默认schema信息
      * root
      * |-- _1: string (nullable = true)
      * |-- _2: integer (nullable = false)
      * |-- _3: integer (nullable = false)
      */

    //自定义schama信息 数据是什么类型 它就是什么类型

    val tpDs2: DataFrame = tpDs.toDF("name","age","fv")

    tpDs2.show()

    //    val wDs: Dataset[String] = file.flatMap(_.split(" "))






  }

}
