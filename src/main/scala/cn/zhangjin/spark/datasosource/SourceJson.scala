package cn.zhangjin.spark.datasosource

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author zhangjin
  * @create 2018-06-13 15:28
  */
object SourceJson {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    //导入SparkSession上面的隐士转换
    import session.implicits._


    //    val config = ConfigFactory.load()

    val json: DataFrame = session.read.json("/Users/zhangjin/myCode/learn/spark-sql/input/jsonlog.json")
    //    json.toDF("pname","price","amout")
    //    json.printSchema()
    json.show()
    val json2: DataFrame = session.read.json("/Users/zhangjin/myCode/learn/spark-sql/input/jsonlog2.json")

    json2.printSchema()
    json2.show()

    //json DSL
    json2.select("address.province").show()

    import  org.apache.spark.sql.functions._

    json2.select(explode($"address"))
//    json2.createTempView("v_tmp")

//    session.sql("select address.city from v_tmp").show()

    session.close()

  }

}
