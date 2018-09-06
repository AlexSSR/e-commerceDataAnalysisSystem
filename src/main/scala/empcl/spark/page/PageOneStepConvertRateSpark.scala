package empcl.spark.page

import com.alibaba.fastjson.JSON
import empcl.constants.Constants
import empcl.dao.factory.DaoFactory
import empcl.helper.JdbcPoolHelper
import empcl.utils.{NumberUtils, ParamUtils, StringUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 页面单跳转化率模块
  */
object PageOneStepConvertRateSpark {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName(Constants.SPARK_APP_NAME_PAGE).setMaster("local[4]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    // 获取任务参数
    val param = DaoFactory.getTaskDao.findById(2).taskParam
    val paramObject = JSON.parseObject(param)
    val startDate = ParamUtils.getParam(paramObject, Constants.PARAM_START_DATE).get
    val endDate = ParamUtils.getParam(paramObject, Constants.PARAM_END_DATE).get
    val targetPageFlow = ParamUtils.getParam(paramObject, Constants.PARAM_TARGET_PAGE_FLOW).get
    val flows = targetPageFlow.split(",")

    import sparkSession.implicits._
    sparkSession.read.parquet("E:\\empcl\\spark-project\\data\\input\\parquet\\user_visit_action\\").createOrReplaceTempView("uservisitaction")
    val sql_session =
      s"""
         |select
         | session_id,
         | page_id
         |from
         | uservisitaction
         |where
         | date >= '$startDate'
         |and
         | date <= '$endDate'
      """.stripMargin
    val sessionId2PageIdDS = sparkSession.sql(sql_session).as[SessionId2PageId]

    /**
      * 需求：根据指定页面流计算页面单跳转化率
      *       按照指定页面流的顺序进行计算
      * 步骤：
      *   1）读取数据，根据session_id进行聚合，返回包含了PageIdCount的数据集，而PageIdCount组成内容为：(page_id: String,count: Int)
      *   2）对1中所得到的数据集根据page_id进行聚合，然后对每组求和，并且按照page_id从小到大进行排序
      *   3）根据targetPageFlow计算出各个页面单跳的转化率，并写入数据库
      */
    //targetPageFlow 3,4,5
    val pageIdCountBufferDS = sessionId2PageIdDS.groupByKey(_.session_id).flatMapGroups((sid, iter) => {
      val flowsBuffer = new ArrayBuffer[PageIdCount]()
      val datas = iter.toArray
      datas.foreach(sp => {
        val page_id = sp.page_id
        if (flows.contains(page_id)) {
          flowsBuffer.append(PageIdCount(page_id, 1))
        }
      })
      flowsBuffer
    })
    val pageIdCountedDS = pageIdCountBufferDS.groupBy("page_id").sum("count")
    val pageIdCountedArr = pageIdCountedDS.sort("page_id").collect()
    var lastPage_id: String = null
    var lastCount: Long = -1L
    val flowCountMap = new mutable.HashMap[String,Double]()
    for (index <- (1 until (pageIdCountedArr.length))) {
      if(StringUtils.isEmpty(lastPage_id)){

        val a = pageIdCountedArr(index - 1)
        lastPage_id = pageIdCountedArr(index - 1).getString(0)
        lastCount = pageIdCountedArr(index - 1).getLong(1)
      }
      val page_id = pageIdCountedArr(index).getString(0)
      val count = pageIdCountedArr(index).getLong(1)
      val rate = NumberUtils.formatDouble(count.toDouble / lastCount.toDouble,2)
      val flow_id = lastPage_id + "_" + page_id
      flowCountMap.put(flow_id,rate)
      lastPage_id = page_id
      lastCount = count
    }
    var rates = ""
    flowCountMap.foreach(map =>{
      val page_id = map._1
      val count = map._2
      rates =  rates + page_id + "=" +count + "|"
    })
    rates = rates.substring(0,rates.length - 1)
    val insert_sql = "insert into page_split_convert_rate values (?,?)"
    JdbcPoolHelper.getJdbcPoolHelper.execute(insert_sql,pstmt =>{
      pstmt.setInt(1,3)
      pstmt.setString(2,rates)

      pstmt.addBatch()
    })


    sparkSession.stop()
  }

}

case class SessionId2PageId(session_id: String, page_id: String)

case class PageIdCount(page_id: String, count: Int)



