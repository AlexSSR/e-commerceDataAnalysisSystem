package empcl

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object Parq2Json {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("Parq2Json").setMaster("local")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    val userVisitActionDF =  sparkSession.read.parquet("E:\\empcl\\spark-project\\data\\input\\parquet\\user_visit_action")
    userVisitActionDF.write.mode(SaveMode.Overwrite).json("E:\\empcl\\spark-project\\data\\input\\json\\user_visit_action")

    sparkSession.stop()

  }

}
