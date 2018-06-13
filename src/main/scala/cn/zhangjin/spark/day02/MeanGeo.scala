package cn.zhangjin.spark.day02

/**
  * @author zhangjin
  * @create 2018-06-13 14:53
  */
object MeanGeo {
  def main(args: Array[String]): Unit = {

    //2 3 4 5 6 求几何平均数

    println(Math.pow(2 * 3 * 5 * 6 * 4, 1D / 5))

    //在集群中运行  ：

    //分而治之

    /**
      * 1 2 3 4 5 6
      * 分区0 1 2   乘积 数据个数  初始值  1  0   1*1 = 1 0+1=1  1*2 = 2 1+1 = 2
      * 分区1 3 4                初始值 1 0    12 2
      * 分区2 5 6                初始值 1 0    30 2
      * 2 * 12 * 30
      * 2 + 2 + 2 = 6
      *先是分区内部计算
      * 分区之间再聚合
      */



  }

}
