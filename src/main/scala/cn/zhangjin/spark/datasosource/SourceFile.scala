package cn.zhangjin.spark.datasosource

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
  * @author zhangjin
  * @create 2018-06-13 15:03
  */
object SourceFile {

  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    //导入SparkSession上面的隐士转换
    import session.implicits._


    //读取普通文件的2个API
    val file = session.read.textFile("/Users/zhangjin/myCode/learn/spark-sql/input/word.txt")
//    val text: DataFrame = session.read.text("/Users/zhangjin/myCode/learn/spark-sql/input/word.txt")

    val word: Dataset[String] = file.flatMap(_.split(" "))

    //save 输出的文件是 parquet文件 有自己的独特的读取方式
    word.write.mode(SaveMode.Append).save("out1")
    word.write.text("out2")
    
    



    session.close()
  }

}
