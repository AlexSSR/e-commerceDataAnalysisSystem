package empcl

import empcl.utils.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
  * 统计数据中某些字段出现的次数
  *
  * @author : empcl
  * @since : 2018/8/8 22:41 ``
  */
object DateTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("DateTest").setMaster("local[4]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    import sparkSession.implicits._
    val userDS = sparkSession.read.json("C:\\empcl\\data\\json\\test\\accumulator\\").as[TestUser]
    val countDS = userDS.map(user => {
      //      val users = iter.toArray
      var r1 = 0
      var r2 = 0
      var r3 = 0
      var r4 = 0
      //      for (user <- users) {
      val u1 = user.user1
      val u2 = user.user2
      val u3 = user.user3
      val u4 = user.user4
      if (StringUtils.isNotEmpty(u1)) {
        r1 = r1 + 1
      }
      if (StringUtils.isNotEmpty(u2)) {
        r2 = r2 + 1
      }
      if (StringUtils.isNotEmpty(u3)) {
        r3 = r3 + 1
      }
      if (StringUtils.isNotEmpty(u4)) {
        r4 = r4 + 1
      }
      Count(r1, r2, r3, r4)
    }


    //      Array(Count(r1, r2, r3, r4)).toIterator

  )

  val resDS = countDS.reduce((c1, c2) => {
    val ri = c1.i + c2.i
    val ri1 = c1.i1 + c2.i1
    val ri2 = c1.i2 + c2.i2
    val ri3 = c1.i3 + c2.i3
    Count(ri, ri1, ri2, ri3)
  })
  println(resDS.i + "," + resDS.i1 + "," + resDS.i2 + "," + resDS.i3)
}

}

case class TestUser(user1: String, user2: String, user3: String, user4: String)

case class Count(i: Int, i1: Int, i2: Int, i3: Int)
