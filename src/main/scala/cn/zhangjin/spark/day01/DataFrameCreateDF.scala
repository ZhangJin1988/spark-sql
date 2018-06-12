package cn.zhangjin.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhangjin
  *         自定义Scahma信息
  */
object DataFrameCreateDF {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[2]").setAppName(this.getClass.getSimpleName)

    val sc: SparkContext = new SparkContext(conf)


    val sqlContext: SQLContext = new SQLContext(sc)

    import sqlContext.implicits._

    val file: RDD[String] = sc.textFile("/Users/zhangjin/myCode/learn/spark-sql/input/person.txt")

    //自定义schema信息
    val schema: StructType = StructType(
      List(
        //字段的名称 类型 是否为空
        StructField("name", StringType, true),
        StructField("age", IntegerType, false),
        StructField("fv", IntegerType, false)
      ))

    //row 是spark sql中的一种类型
    val rowRdd: RDD[Row] = file.map(_.split(" ")).map(t => Row(t(0), t(1).toInt, t(2).toInt))
    val pdf: DataFrame = sqlContext.createDataFrame(rowRdd, schema)

    pdf.printSchema()
    pdf.registerTempTable("t_person")
    val result: DataFrame = sqlContext.sql("select * from t_person where fv > 90")
//    val result: DataFrame = pdf.select("")
    result.show()


    sc.stop()


  }


}
