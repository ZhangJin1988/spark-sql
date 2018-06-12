package cn.zhangjin.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhangjin
  * @create 2018-06-12 15:21
  */
object DataFrameDemo1 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[2]").setAppName(this.getClass.getSimpleName)

    val sc: SparkContext = new SparkContext(conf)



    val file: RDD[String] = sc.textFile("/Users/zhangjin/myCode/learn/spark-sql/input/person.txt")

//    file.map()












  }

}

case class Person(name:String,age:Int,fv:Int)
