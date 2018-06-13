package cn.zhangjin.spark.day02

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @author zhangjin
  */
object JoinDemo {

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

    //如何通过  name age province
    val ds1: Dataset[String] = session.createDataset(List("bxc 28 jz", "ssby 1000 eng", "cangls 18 jp"))

    //数据预处理
    val user: DataFrame = ds1.map(str => {
      val split = str.split(" ")
      val name = split(0)
      val age = split(1)
      val pcode = split(2)
      (name, age, pcode)
    }).toDF("name", "age", "pcode")
    //pcode province
    val ds2: Dataset[String] = session.createDataset(List("jz 京州市", "eng 英国", "jp 日本省", "hn 海南省"))
    val province: DataFrame = ds2.map(str => {
      val split = str.split(" ")
      (split(0), split(1))
    }).toDF("pcode", "province")
    //如果我们使用sql风格  注册临时表  注册多张表 临时视图 再进行关联查询
    user.createTempView("v_user")
    province.createTempView("v_pro")
    val result1: DataFrame = session.sql("select * from v_user t1 join v_pro t2 on t1.pcode = t2.pcode ")

//    result1.show()


    // DSL 的风格 先join 再where 关联条件 ===
//    user.join(province).where(user("pcode") === province("pcode")).show()


//    user.join(province,"pcode").show()

    user.join(province,user("pcode") === province("pcode"),"right").show()
    session.close()
  }

}
