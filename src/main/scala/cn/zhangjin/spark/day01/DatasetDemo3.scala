package cn.zhangjin.spark.day01

import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, SparkSession}

/**
  * @author zhangjin
  * @create 2018-06-12 17:30
  */
object DatasetDemo3 {

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

    val pdf: DataFrame = file.map(_.split(" ")).map(t => (t(0), t(1).toInt, t(2).toInt)).toDF("name", "age", "fv")


    //    val tpDs: Dataset[(String, Int, Int)] = pdf

    //dsl 语法
    // select where group by order by count sum max min
    //选择

    pdf.select("name", "age").show()
    //    pdf.select(pdf.col("name"), pdf.col("age")).show()
    //    pdf.select(pdf("name"), pdf("age")).show()

    //where 过滤 dsl风格
    pdf.where("fv > 90 ").show()
    pdf.filter("fv > 90").show()


    //排序 order by  rdd算子中 叫做 sortby
    pdf.orderBy("age").show()
    pdf.sort("age").show()

    //多个条件的排序
    pdf.sort($"age", $"fv" desc).show()

    //分组之后还需要进行聚合统计
    pdf.groupBy($"age").sum("fv").show()


    val result1: DataFrame = pdf.groupBy($"age").count()
    result1.show()
    //聚合条件  需要导入函数 function
    import org.apache.spark.sql.functions._
    pdf.groupBy($"age").agg(count("*") as "cnts").show()

    pdf.groupBy($"age").agg(max("fv")).show()


    // 重命名列名

    pdf.withColumnRenamed("age","myage").show()

    // 以SQL为主

    session.stop()


  }

}
