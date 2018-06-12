package cn.zhangjin.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * @author zhangjin
  * @create 2018-06-12 16:32
  */
object DataFrameWC {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)
    import sqlContext.implicits._
    val words: RDD[String] = sc.textFile("/Users/zhangjin/myCode/learn/spark-sql/input/word.txt")
    val wordsRdd: RDD[String] = words.flatMap(_.split(" "))
    val wordDf: DataFrame = wordsRdd.map(Word(_)).toDF()
//    wordsRdd.map(t =>Word(t))

    wordDf.registerTempTable("5")

    val result: DataFrame = sqlContext.sql(" select name ,count(*) as cnts from t_word group by name order by cnts desc ")
    result.show()

  }

}


//定义一个字段
case class Word(name: String)
