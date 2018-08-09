package empcl.utils

/**
  * @author : empcl
  * @since : 2018/8/9 9:44 
  */
object NumberUtils {

  /**
    * 格式化小数 四舍五入
    *
    * @param num 需要格式化的数
    * @param scale 四舍五入的位数
    * @return 格式化小数
    */
  def formatDouble(num: Double,scale: Int): Double = {

    val pattern = "%."+scale+"f"
    num.formatted(pattern).toDouble

  }

}
