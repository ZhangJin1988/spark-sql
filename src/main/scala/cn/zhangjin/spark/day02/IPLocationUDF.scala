package cn.zhangjin.spark.day02

import java.util.Properties

import cn.zhangjin.utils.IpUtils
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
  * @author zhangjin
  * @create 2018-06-13 11:31
  */
object IPLocationUDF {


  def main(args: Array[String]): Unit = {

    //scala akka
    //application.conf ---> application.json ---> application.properties

    val config: Config = ConfigFactory.load()
    val str = config.getString("db.url")
    println(str)


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
    //    val ds1: Dataset[String] = session.createDataset(List("bxc 28 jz", "ssby 1000 eng", "cangls 18 jp"))

    val ipDS: Dataset[String] = session.read.textFile("/Users/zhangjin/myCode/learn/spark-sql/input/ip.txt")
    val logsDS: Dataset[String] = session.read.textFile("/Users/zhangjin/myCode/learn/spark-sql/input/ipaccess.log")

    val ipRulesDF: DataFrame = ipDS.map(t => {
      val split = t.split("\\|")
      (split(2).toLong, split(3).toLong, split(6))
    }).toDF("start", "end", "province")

    val ipRulesDFs: Dataset[(Long, Long, String)] = ipDS.map(t => {
      val split = t.split("\\|")
      (split(2).toLong, split(3).toLong, split(6))
    })

    val longIpDF: DataFrame = logsDS.map(str => {
      IpUtils.ip2Long(str.split("\\|")(1))
    }).toDF("longIp")
    ipRulesDF.createTempView("v_iprules")
    longIpDF.createTempView("v_longIp")


    //UDF 自定义函数
    //    val tuples: Array[(Long, Long, String)] = ipRulesDF.rdd.map(t => {
    //      val start: Long = t.getAs[Long]("start")
    //      val end: Long = t.getAs[Long]("end")
    //      val province: String = t.getAs[String]("province")
    //      (start, end, province)
    //    }).collect()
    //    tuples

    //把RDD中的数据 收集成数组
    val ipRules: Array[(Long, Long, String)] = ipRulesDFs.collect()
    session.udf.register("ip2Province", (longIp: Long) => {
      //通过：LongIp获取到省份
      IpUtils.binarySearchProvince(longIp, ipRules)

    })


    val result: DataFrame = session.sql("select ip2Province(longIp) as  province , count(*) as cnts from v_iprules t1 join v_longIp t2 on t2.longIp between t1.start and t1.end group by province  order by cnts desc ")
    //    result

    //    val result: DataFrame = session.sql("select province , count(*) as cnts from v_iprules t1 join v_longIp t2 on t2.longIp between t1.start and t1.end " +
    //      "group by province  order by cnts desc "
    //    )
    //    result.show()

    //    com.mysql.jdbc.driver

    //自动创建的表的结构 就是 我们DataFrame的scheema信息中的字段
    val url = config.getString("db.url")
    val table = config.getString("db.table")
    val conn = new Properties()
    conn.setProperty("user", config.getString("db.user"))
    conn.setProperty("password", config.getString("db.pwd"))

    //Driver  提交到集群中
    conn.setProperty("driver", "com.mysql.jdbc.Driver")

    result.write.mode(SaveMode.Overwrite).jdbc(url, table, conn)


    session.close()
  }
}
