package cn.zhangjin.utils

/**
  * @author zhangjin
  * @create 2018-06-11 20:03
  */
object IpUtils {


  // 定义一个方法，把ip地址转换为10进制
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  // 定义一个二分搜索的方法
  def binarySearchProvince(longIp: Long, ipRules: Array[(Long, Long, String)]): String = {
    // 定义两个索引
    var low = 0
    var high = ipRules.length - 1

    // 只要满足条件，就进行二分搜索
    while (low <= high) {
      //中间值的索引
      val middle = (low + high) / 2
      // 接收 middle位置的值
      val (start, end, province) = ipRules(middle)
      if (longIp >= start && longIp <= end) {
        // 找到了，就直接返回省份值
        // 利用return关键字返回省份结果
        return province
      } else if (longIp < start) { // 要查找的值在左区间
        // 缩小查找范围，把high 调小
        high = middle - 1
      } else {
        low = middle + 1
      }
    }
    // 如果程序运行到这里，没有找到结果
    "unknown"
  }


  // 定义一个二分搜索的方法
  def binarySearchProvinceAndCity(longIp: Long, ipRules: Array[(Long, Long, String, String)]): (String, String) = {
    // 定义两个索引
    var low = 0
    var high = ipRules.length - 1

    // 只要满足条件，就进行二分搜索
    while (low <= high) {
      //中间值的索引
      val middle = (low + high) / 2
      // 接收 middle位置的值
      val (start, end, province, city) = ipRules(middle)
      if (longIp >= start && longIp <= end) {
        // 找到了，就直接返回省份值
        // 利用return关键字返回省份结果
        return (province, city)
      } else if (longIp < start) { // 要查找的值在左区间
        // 缩小查找范围，把high 调小
        high = middle - 1
      } else {
        low = middle + 1
      }
    }
    // 如果程序运行到这里，没有找到结果
    ("unknown", "unknown")
  }


}
