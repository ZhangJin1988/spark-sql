package cn.zhangjin.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhangjin
  * @create 2018-06-12 16:21
  */
object WordCountDataFrame {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[2]").setAppName(this.getClass.getSimpleName)

    val sc: SparkContext = new SparkContext(conf)


    val sqlContext: SQLContext = new SQLContext(sc)


    import  sqlContext.implicits._
    val file: RDD[String] = sc.textFile("/Users/zhangjin/myCode/learn/spark-sql/input/word.txt")


    val lines: RDD[String] = file.flatMap(k=>k.split(" "))

    val rows=lines.map { x => Row(x) }


    val field=Array(DataTypes.createStructField("name", DataTypes.StringType, true))
    val structType=DataTypes.createStructType(field)


    val df=sqlContext.createDataFrame(rows, structType)

    df.registerTempTable("t_word")
    sqlContext.udf.register("str", (name:String)=>1)

    sqlContext.sql("select name,str(name) from t_word ").groupBy(df.col("name")).count().show

    sc.stop()


  }

}
