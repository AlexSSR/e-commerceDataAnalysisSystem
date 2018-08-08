package empcl.utils

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date}

/**
  * @author : empcl
  * @since : 2018/8/8 22:27 
  */
object DateUtils {

  val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
  val DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd")


  /**
    * 判断一个时间在另一个时间之前
    *
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def before(time1: String, time2: String): Option[Boolean] = {
    var flag = false
    try {
      val dateTime1 = TIME_FORMAT.parse(time1)
      val dateTime2 = TIME_FORMAT.parse(time2)
      if (dateTime1.before(dateTime2)) {
        flag = true
      }
      Some(flag)
    } catch {
      case ex: ParseException => {
        ex.printStackTrace()
        None
      }
    }
  }

  /**
    * 判断一个时间在另一个时间之后
    *
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def after(time1: String, time2: String): Option[Boolean] = {
    var flag = false
    try {
      val dateTime1 = TIME_FORMAT.parse(time1)
      val dateTime2 = TIME_FORMAT.parse(time2)
      if (dateTime1.after(dateTime2)) {
        flag = true
      }
      Some(flag)
    } catch {
      case ex: ParseException => {
        ex.printStackTrace()
        None
      }
    }
  }

  /**
    * 计算时间差值（单位为秒）
    *
    * @param time1 时间1
    * @param time2 时间2
    * @return 差值
    */
  def minus(time1: String, time2: String): Option[Long] = {
    try {
      val dateTime1 = TIME_FORMAT.parse(time1)
      val dateTime2 = TIME_FORMAT.parse(time2)
      val d1 = dateTime1.getTime
      val d2 = dateTime2.getTime
      val result = (d1 - d2) / 1000
      Some(result)
    } catch {
      case ex => {
        ex.printStackTrace()
        None
      }
    }
  }

  /**
    * 获取年月日和小时
    *
    * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
    * @return 结果（yyyy-MM-dd_HH）
    */
  def getDateHour(datetime: String): String = {
    val dt = datetime.split(" ")
    val hour = dt(1).split(":")(0)
    dt(0) + "_" + hour
  }

  /**
    * 获取当天日期（yyyy-MM-dd）
    *
    * @return 当天日期
    */
  def getTodayDate(): String = {
    TIME_FORMAT.format(new Date())
  }

  /**
    * 获取昨天的日期（yyyy-MM-dd）
    *
    * @return 昨天的日期
    */
  //TODO 学习Calendar
  def getYesterdayDate(): String = {
    val calendar = Calendar.getInstance()
    calendar.setTime(new Date())
    calendar.add(Calendar.DAY_OF_YEAR, -1)
    val date = calendar.getTime
    TIME_FORMAT.format(date)
  }

  /**
    * 格式化时间（yyyy-MM-dd HH:mm:ss）
    *
    * @param date Date对象
    * @return 格式化后的时间
    */
  def formatTime(date: Date): String = {
    TIME_FORMAT.format(date)
  }

  /**
    * 解析时间字符串
    * @param time 时间字符串
    * @return Date
    */
  def parseTime(time: String): Option[Date] = {
    try {
      Some(TIME_FORMAT.parse(time))
    } catch {
      case ex => {
        ex.printStackTrace()
        None
      }
    }
  }

  /**
    * 格式化日期key
    * @param date
    * @return
    */
  def formatDateKey(date: Date): String = {
    DATEKEY_FORMAT.format(date)
  }

  /**
    * 格式化时间，保留到分钟级别
    * yyyyMMddHHmm
    * @param date
    * @return
    */
  def formatTimeMinute(date: Date): String = {
    val sdf = new SimpleDateFormat("yyyyMMddHHmm")
    sdf.format(date)
  }


}
