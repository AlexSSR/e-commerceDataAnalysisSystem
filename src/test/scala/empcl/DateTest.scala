package empcl

import java.util.Date

import empcl.utils.DateUtils

/**
  * @author : empcl
  * @since : 2018/8/8 22:41 
  */
object DateTest {

  def main(args: Array[String]): Unit = {

    /*val b = DateUtils.before("a","b")
    println(b)*/
    //    val datetime = "2018-22-23 22:11:33"
    val datetime = "a"
    /*println(DateUtils.getDateHour(datetime))
    val date = new Date()
    println(date)
    println(DateUtils.getYesterdayDate())*/
    println(DateUtils.parseTime(datetime).getOrElse("no"))
  }

}
