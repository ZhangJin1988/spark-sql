package cn.zhangjin.spark.datasosource

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author zhangjin
  * @create 2018-06-13 16:06
  *        @desc 读取csv格式的数据
  */
object SourceCsv {

  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    //导入SparkSession上面的隐士转换
    import session.implicits._


    val json: DataFrame = session.read.json("/Users/zhangjin/myCode/learn/spark-sql/input/jsonlog.json")
//    json.write.csv("/Users/zhangjin/myCode/learn/spark-sql/out9")


    val csv: DataFrame = session.read.csv("/Users/zhangjin/myCode/learn/spark-sql/out9")
    csv.printSchema()

    //默认的schema信息 都是下划线 _c0 _c1 _c2 .... 都是String类型的
    csv.write.parquet("/Users/zhangjin/myCode/learn/spark-sql/out8")
    session.close()
  }

}
