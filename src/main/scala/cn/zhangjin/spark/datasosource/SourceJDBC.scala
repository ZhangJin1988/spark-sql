package cn.zhangjin.spark.datasosource

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._

/**
  * @author zhangjin
  * @create 2018-06-13 15:10
  */
object SourceJDBC {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    //导入SparkSession上面的隐士转换
    import session.implicits._


    val config = ConfigFactory.load()


    val conn = new Properties()
    conn.setProperty("user", config.getString("db.user"))
    conn.setProperty("password", config.getString("db.pwd"))
    conn.setProperty("driver", config.getString("db.driver"))
    val mysql1: DataFrame = session.read.jdbc(config.getString("db.url"), "order_1", conn)

    //读取mysql之后 获取到schema之后 mysql表结构信息是一致的
    val result2: Dataset[Row] = mysql1.where("num > 300")
//    result2.show()
//    result2.write.mode(SaveMode.Append).jdbc(config.getString("db.url"),"order_8",conn)
    //尝试把数据写入到普通文件  text只能写成单例
//    result2.write.text("out3")

    //DataFrame Dataset 都可以通过.rdd方法 转成rdd
    //DataFrame 转换成的rdd 是 RDD[Row] 类型
    //dataset 转换成的RDD 和 原有的Dataset的数据类型是一致的

    //Dataset 和 DataFrame 都可以通过rdd方法
    result2.rdd.saveAsTextFile("out3")
//    mysql1.printSchema()
    session.close()
  }

}
