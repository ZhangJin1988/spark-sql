package cn.zhangjin.spark.datasosource

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author zhangjin
  * @create 2018-06-13 16:19
  */
object SourceParquet {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    //导入SparkSession上面的隐士转换
    import session.implicits._


    val parquet: DataFrame = session.read.parquet("/Users/zhangjin/myCode/learn/spark-sql/out8")

    parquet.show()

    val frame: DataFrame = session.read.parquet("/Users/zhangjin/myCode/learn/spark-sql/out9")

    session.close()

  }
}
