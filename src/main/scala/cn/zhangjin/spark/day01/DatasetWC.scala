package cn.zhangjin.spark.day01

import org.apache.spark.SparkContext
import org.apache.spark.sql._

/**
  * @author zhangjin
  * @create 2018-06-12 16:42
  */
object DatasetWC {

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
    val file: Dataset[String] = session.read.textFile("/Users/zhangjin/myCode/learn/spark-sql/input/word.txt")

    val wDs: Dataset[String] = file.flatMap(_.split(" "))

    //dataset 默认有schema信息 到底是什么  就叫做String 默认shema的名称是value

    wDs.printSchema()
    //    root
    //    |-- value: string (nullable = true)

    //    file.


    //sql 语法风格  dsl语法风格

    //sql 语法

    //注册临时表
    wDs.createTempView("v_word")


    //    val result: Dataset[Row] = session.sql("select value,count(*) cnts from v_word group by value order by cnts desc ")
    val result: DataFrame = session.sql("select value,count(*) cnts from v_word group by value order by cnts desc ")


    result.show()

    // 再用flatmap算子操作的时候 需要导入隐士转换
    //    Error:(29, 43) Unable to find encoder for type stored in a Dataset.  Primitive types (Int, String, etc) and Product types (case classes) are supported by importing spark.implicits._  Support for serializing other types will be added in future releases.
    //    val wDs:Dataset[String] = file.flatMap(_.split(" "))


    session.stop()
    //    session.close()


  }

}
