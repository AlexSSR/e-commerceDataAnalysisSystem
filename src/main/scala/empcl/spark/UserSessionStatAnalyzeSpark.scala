package empcl.spark

import java.util.Date

import com.alibaba.fastjson.JSON
import empcl.constants.Constants
import empcl.dao.factory.DaoFactory
import empcl.helper.JdbcPoolHelper
import empcl.utils._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random


/**
  * 1. 按照session粒度进行数据聚合
  * 2. 根据每个session的时长，步长区间分布统计出共有多少个session，以及各个区间上面session占得比重是多少
  *
  * @author : empcl
  * @since : 2018/8/11 16:18 
  */
object UserSessionStatAnalyzeSpark {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setAppName(Constants.SPARK_APP_NAME).setMaster("local")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    import sparkSession.implicits._

    val userVistActionDF = sparkSession.read.parquet("C:\\empcl\\data\\input\\parquet\\user_visit_action\\")
    val userInfoDF = sparkSession.read.parquet("C:\\empcl\\data\\input\\parquet\\user_info")
    userVistActionDF.createOrReplaceTempView("userVisitAction")
    userInfoDF.createOrReplaceTempView("userInfo")

    val task = DaoFactory.getTaskDao.findById(1)
    val param = task.taskParam
    val taskParam = JSON.parseObject(param)
    val startDate = ParamUtils.getParam(taskParam, "startDate").getOrElse("2015-08-11 11:33:44")
    val endDate = ParamUtils.getParam(taskParam, "endDate").getOrElse("2025-08-11 11:33:44")
    val startAge = ParamUtils.getParam(taskParam, "startAge")
    val endAge = ParamUtils.getParam(taskParam, "endAge")
    val professional = ParamUtils.getParam(taskParam, "professional")
    val city = ParamUtils.getParam(taskParam, "city")
    val sex = ParamUtils.getParam(taskParam, "sex")
    val searchKeyWords = ParamUtils.getParam(taskParam, "searchKeyWords")
    val clickCategory = ParamUtils.getParam(taskParam, "clickCategory")

    // 拼接任务参数
    var parameter = (if (startAge.isDefined) Constants.PARAM_START_AGE + "=" + startAge.get + "|" else "") +
      (if (endAge.isDefined) Constants.PARAM_END_AGE + "=" + endAge.get + "|" else "") +
      (if (professional.isDefined) Constants.PARAM_PROFESSIONAL + "=" + professional.get + "|" else "") +
      (if (city.isDefined) Constants.PARAM_CITY + "=" + city.get + "|" else "") +
      (if (sex.isDefined) Constants.PARAM_SEX + "=" + sex.get + "|" else "") +
      (if (searchKeyWords.isDefined) Constants.PARAM_SEARCHKEYWORDS + "=" + searchKeyWords.get + "|" else "") +
      (if (clickCategory.isDefined) Constants.PARAM_CLICKCATEGORY + "=" + clickCategory.get + "|" else "")
    if (parameter.endsWith("|")) {
      parameter = parameter.substring(0, parameter.length - 1)
    }

    val sql4VisitAction =
      s"""
         |SELECT
         | session_id,
         | user_id,
         | action_time,
         | search_keyword,
         | click_category_id,
         | page_id,
         | click_product_id,
         | order_category_ids,
         | order_product_ids,
         | pay_category_ids,
         | pay_product_ids
         |FROM
         | userVisitAction
         |WHERE
         | date >= '$startDate'
         |AND
         | date <= '$endDate'
      """.stripMargin

    val sql4Userinfo =
      """
        |SELECT
        | user_id,
        | username,
        | name,
        | age,
        | professional,
        | city,
        | sex
        |FROM
        | userInfo
      """.stripMargin

    val sessionInfoDS = sparkSession.sql(sql4VisitAction).as[SessionFullInfo]
    val userInfoDS = sparkSession.sql(sql4Userinfo).as[UserInfo]

    // 缓存基础DS数据
    sessionInfoDS.persist()

    val sessionFullAggrInfoDS = sessionInfoDS.join(userInfoDS, "user_id")

    // 按照年龄范围,职业,城市,性别,搜索词,点击品类过滤
    val filtered2FullInfoDS = sessionFullAggrInfoDS.filter(row => {
      /**
        * |-- user_id: long (nullable = true) 0
        * |-- session_id: string (nullable = true) 1
        * |-- action_time: string (nullable = true) 2
        * |-- search_keyword: string (nullable = true) 3
        * |-- click_category_id: long (nullable = true) 4
        * |-- page_id: long (nullable = true) 5
        * |-- click_product_id: long (nullable = true) 6
        * |-- order_category_ids: string (nullable = true) 7
        * |-- order_product_ids: string (nullable = true) 8
        * |-- pay_category_ids: string (nullable = true) 9
        * |-- pay_product_ids: string (nullable = true) 10
        * |-- username: string (nullable = true) 11
        * |-- name: string (nullable = true) 12
        * |-- age: integer (nullable = true) 13
        * |-- professional: string (nullable = true) 14
        * |-- city: string (nullable = true) 15
        * |-- sex: string (nullable = true) 16
        *
        */
      var flag = true
      val keyWordsInfo = row.getString(3)
      val clickCategoryInfo = row.get(4)
      val age = row.getInt(13).toString
      val professional = row.getString(14)
      val city = row.getString(15)
      val sex = row.getString(16)

      // 按照年龄过滤
      val _startAge = StringUtils.getFieldFromConcatString(parameter, "\\|", Constants.PARAM_START_AGE).getOrElse("")
      val _endAge = StringUtils.getFieldFromConcatString(parameter, "\\|", Constants.PARAM_END_AGE).getOrElse("")
      if (!ValidUtils.between(age, _startAge, _endAge)) {
        flag = false
      }
      // 按照职业过滤
      val _professional = StringUtils.getFieldFromConcatString(parameter, "\\|", Constants.PARAM_PROFESSIONAL).getOrElse("")
      if (!ValidUtils.in(professional, _professional)) {
        flag = false
      }
      // 按照城市过滤
      val _city = StringUtils.getFieldFromConcatString(parameter, "\\|", Constants.PARAM_CITY).getOrElse("")
      if (!ValidUtils.in(city, _city)) {
        flag = false
      }
      // 按照性别过滤
      val _sex = StringUtils.getFieldFromConcatString(parameter, "\\|", Constants.PARAM_SEX).getOrElse("")
      if (!ValidUtils.equals(sex, _sex)) {
        flag = false
      }
      // 按照搜索词过滤
      val searchKeyWords = StringUtils.getFieldFromConcatString(parameter, "\\|", Constants.PARAM_SEARCHKEYWORDS).getOrElse("")
      if (!ValidUtils.in(keyWordsInfo, searchKeyWords)) {
        flag = false
      }
      // 按照点击品类进行过滤
      val _clickCategory = StringUtils.getFieldFromConcatString(parameter, "\\|", Constants.PARAM_CLICKCATEGORY).getOrElse("")
      if (!ValidUtils.in(clickCategoryInfo, _clickCategory)) {
        flag = false
      }
      flag
    })

    filtered2FullInfoDS.persist()

    // 按照session_id 进行聚合数据
    // user_id|session_id|action_time|search_keyword|click_category_id|username|name
    val sessionId2FullAggrInfoDS = filtered2FullInfoDS.groupByKey(_.getString(1)).mapGroups((sessionId, iter) => {

      val datas = iter.toArray

      var startTime: Date = null
      var endTime: Date = null

      for (i <- datas.indices) {
        val data = datas(i)

        val actionTime = DateUtils.parseTime(data.getString(2)).get
        if (startTime == null) {
          startTime = actionTime
        }
        if (endTime == null) {
          endTime = actionTime
        }
        if (actionTime.before(startTime)) {
          startTime = actionTime
        }
        if (actionTime.after(endTime)) {
          endTime = actionTime
        }
      }

      val visitLength = ((endTime.getTime - startTime.getTime) / 1000).toShort
      val stepLength: Short = datas.length.toShort


      var aggrLengthInfo = Constants.TIME_PERIOD_1s_3s + "=0|" +
        Constants.TIME_PERIOD_4s_6s + "=0|" +
        Constants.TIME_PERIOD_7s_9s + "=0|" +
        Constants.TIME_PERIOD_10s_30s + "=0|" +
        Constants.TIME_PERIOD_30s_60s + "=0|" +
        Constants.TIME_PERIOD_1m_3m + "= 0|" +
        Constants.TIME_PERIOD_3m_10m + "=0|" +
        Constants.TIME_PERIOD_10m_30m + "=0|" +
        Constants.TIME_PERIOD_30m + "=0|" +
        Constants.STEP_PERIOD_1_3 + "=0|" +
        Constants.STEP_PERIOD_4_6 + "=0|" +
        Constants.STEP_PERIOD_7_9 + "=0|" +
        Constants.STEP_PERIOD_10_30 + "=0|" +
        Constants.STEP_PERIOD_30_60 + "=0|" +
        Constants.STEP_PERIOD_60 + "=0"

      // 判断visitLength属于哪个区间
      aggrLengthInfo = calculateVisitLength(visitLength, aggrLengthInfo)
      // 判断stepLength属于哪个区间
      aggrLengthInfo = calculateStepLength(stepLength, aggrLengthInfo)

      val time_s1_3: Int = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_1s_3s).get.toInt
      val time_s4_6: Int = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_4s_6s).get.toInt
      val time_s7_9: Int = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_7s_9s).get.toInt
      val time_s10_30: Int = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_10s_30s).get.toInt
      val time_s30_60: Int = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_30s_60s).get.toInt
      val time_m1_3: Int = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_1m_3m).get.toInt
      val time_m3_10: Int = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_3m_10m).get.toInt
      val time_m10_30: Int = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_10m_30m).get.toInt
      val time_m30: Int = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_30m).get.toInt
      val step_1_3: Int = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.STEP_PERIOD_1_3).get.toInt
      val step_4_6: Int = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.STEP_PERIOD_4_6).get.toInt
      val step_7_9: Int = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.STEP_PERIOD_7_9).get.toInt
      val step_10_30: Int = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.STEP_PERIOD_10_30).get.toInt
      val step_30_60: Int = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.STEP_PERIOD_30_60).get.toInt
      val step_60: Int = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.STEP_PERIOD_60).get.toInt

      SessionAggrStat(1, time_s1_3, time_s4_6, time_s7_9, time_s10_30,
        time_s30_60, time_m1_3, time_m3_10, time_m10_30, time_m30, step_1_3, step_4_6,
        step_7_9, step_10_30, step_30_60, step_60)
    })


    // 聚合各个DS中的session相关信息落于时间区间，步数区间的情况
    val aggredSessionInfo = sessionId2FullAggrInfoDS.reduce((s1, s2) => {
      val count = s1.sessionNum + s2.sessionNum
      val s1_3 = s1.time_s1_3 + s2.time_s1_3
      val s4_6 = s1.time_s4_6 + s2.time_s4_6
      val s7_9 = s1.time_s7_9 + s2.time_s7_9
      val s10_30 = s1.time_s10_30 + s2.time_s10_30
      val s30_60 = s1.time_s30_60 + s2.time_s30_60
      val m1_3 = s1.time_m1_3 + s2.time_m1_3
      val m3_10 = s1.time_m3_10 + s2.time_m3_10
      val m10_30 = s1.time_m10_30 + s2.time_m10_30
      val m_30 = s1.time_m30 + s2.time_m30
      val step_1_3 = s1.step_1_3 + s2.step_1_3
      val step_4_6 = s1.step_4_6 + s2.step_4_6
      val step_7_9 = s1.step_7_9 + s2.step_7_9
      val step_10_30 = s1.step_10_30 + s2.step_10_30
      val step_30_60 = s1.step_30_60 + s2.step_30_60
      val step_60 = s1.step_60 + s2.step_60
      SessionAggrStat(count, s1_3, s4_6, s7_9, s10_30,
        s30_60, m1_3, m3_10, m10_30, m_30, step_1_3, step_4_6,
        step_7_9, step_10_30, step_30_60, step_60)
    })

    // 将过滤后的数据写入到数据库中..
    val insertSql = "INSERT INTO session_aggr_stat values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    JdbcPoolHelper.getJdbcPoolHelper.execute(insertSql, pstmt => {

      val sessionNums = aggredSessionInfo.sessionNum
      pstmt.setInt(1, task.taskId)
      pstmt.setInt(2, sessionNums)
      pstmt.setDouble(3, NumberUtils.formatDouble(aggredSessionInfo.time_s1_3.toDouble / sessionNums.toDouble, 2))
      pstmt.setDouble(4, NumberUtils.formatDouble(aggredSessionInfo.time_s4_6.toDouble / sessionNums.toDouble, 2))
      pstmt.setDouble(5, NumberUtils.formatDouble(aggredSessionInfo.time_s7_9.toDouble / sessionNums.toDouble, 2))
      pstmt.setDouble(6, NumberUtils.formatDouble(aggredSessionInfo.time_s10_30.toDouble / sessionNums.toDouble, 2))
      pstmt.setDouble(7, NumberUtils.formatDouble(aggredSessionInfo.time_s30_60.toDouble / sessionNums.toDouble, 2))
      pstmt.setDouble(8, NumberUtils.formatDouble(aggredSessionInfo.time_m1_3.toDouble / sessionNums.toDouble, 2))
      pstmt.setDouble(9, NumberUtils.formatDouble(aggredSessionInfo.time_m3_10.toDouble / sessionNums.toDouble, 2))
      pstmt.setDouble(10, NumberUtils.formatDouble(aggredSessionInfo.time_m10_30.toDouble / sessionNums.toDouble, 2))
      pstmt.setDouble(11, NumberUtils.formatDouble(aggredSessionInfo.time_m30.toDouble / sessionNums.toDouble, 2))
      pstmt.setDouble(12, NumberUtils.formatDouble(aggredSessionInfo.step_1_3.toDouble / sessionNums.toDouble, 2))
      pstmt.setDouble(13, NumberUtils.formatDouble(aggredSessionInfo.step_4_6.toDouble / sessionNums.toDouble, 2))
      pstmt.setDouble(14, NumberUtils.formatDouble(aggredSessionInfo.step_7_9.toDouble / sessionNums.toDouble, 2))
      pstmt.setDouble(15, NumberUtils.formatDouble(aggredSessionInfo.step_10_30.toDouble / sessionNums.toDouble, 2))
      pstmt.setDouble(16, NumberUtils.formatDouble(aggredSessionInfo.step_30_60.toDouble / sessionNums.toDouble, 2))
      pstmt.setDouble(17, NumberUtils.formatDouble(aggredSessionInfo.step_60.toDouble / sessionNums.toDouble, 2))

      //      pstmt.addBatch()

    })

    /**
      * 抽取的session总数记为100个
      * 按每个小时占得一天总session的比例，随机抽取session
      */
    //从数据库读取共有多少条数据
    val querySql = "select session_count from session_aggr_stat where task_id = ?"
    val params: Array[Any] = Array(1)
    val session_countOpt = JdbcPoolHelper.getJdbcPoolHelper.executeQuery(querySql, params, (params, pstmt) => {
      pstmt.setInt(1, params(0).toString.toInt)
    },
      rs => {
        if (rs.next()) {
          val session_count = rs.getInt(1)
          Some(session_count)
        } else {
          None
        }
      })
    val session_count = session_countOpt.getOrElse(0).toString

    // 获取(session_id,time,start_time)
    val SessionHourDS = filtered2FullInfoDS.groupByKey(_.getString(1)).mapGroups((session_id, iter) => {
      var startTime: Date = null
      var actionTime: Date = null
      val datas = iter.toArray
      for (index <- datas.indices) {
        actionTime = DateUtils.parseTime(datas(index).getString(2)).get
        if (startTime == null) {
          startTime = actionTime
        }
        if (actionTime.before(startTime)) {
          startTime = actionTime
        }
      }
      val time = DateUtils.getHour(DateUtils.formatTime(startTime))
      val startTimeStr = DateUtils.formatTime(startTime)
      SessionHour(session_id, time, startTimeStr)
    })

    val random = new Random()
    val HourSessionIndexsDS = SessionHourDS.groupByKey(_.time).mapGroups((hh, iter) => {
      val datas = iter.toArray
      val len = datas.length
      val count = ((len.toDouble / session_count.toDouble) * 100).toInt

      // 随机抽取session的索引，存储在String字符串中
      var indexsBuffer = new ArrayBuffer[Int]

      for (i <- (0 until (count))) {
        var extractIndex = random.nextInt(len)
        while (indexsBuffer.contains(extractIndex)) {
          extractIndex = random.nextInt(len)
        }
        indexsBuffer.append(extractIndex)
      }

      ExtractHourSessionIndex(hh, indexsBuffer.mkString(","))
    })

    // 聚合出click_category_id,click_category_ids这样的数据
    val ExtractHourSessionDS = filtered2FullInfoDS.groupByKey(_.getString(1)).mapGroups((session_id, iter) => {

      val datas = iter.toArray

      var startTime: Date = null
      var actionTime: Date = null
      val search_keywords = new ArrayBuffer[String]
      val click_category_ids = new ArrayBuffer[String]
      var start_search_keyword = ""
      var start_click_category_id = ""
      var search_keywords_Str = ""
      var click_category_ids_Str = ""

      for (i <- datas.indices) {

        val data = datas(i)
        val search_keyword = data.getString(3)
        val click_category_id = data.getString(4)
        actionTime = DateUtils.parseTime(data.getString(2)).get

        if (StringUtils.isNotEmpty(search_keyword)) {
          search_keywords.append(search_keyword)
        }
        if (StringUtils.isNotEmpty(click_category_id)) {
          click_category_ids.append(click_category_id)
        }
        if (startTime == null) {
          startTime = actionTime
          start_search_keyword = search_keyword
          start_click_category_id = click_category_id
        }
        if (actionTime.before(startTime)) {
          startTime = actionTime
          start_search_keyword = search_keyword
          start_click_category_id = click_category_id
        }
      }
      val formatTime = DateUtils.formatTime(startTime)
      val hour = formatTime.split(" ")(1).split(":")(0)
      search_keywords_Str = search_keywords.mkString(",")
      click_category_ids_Str = click_category_ids.mkString(",")

      ExtractHourSession(hour, session_id, formatTime, search_keywords_Str, click_category_ids_Str)
    })

    // 将HourSessionIndexsDS与filtered2FullInfoDS进行join
    val ExtractHourIndex2SessionsDS = HourSessionIndexsDS.join(ExtractHourSessionDS, "hour")

    ExtractHourIndex2SessionsDS.write.mode(SaveMode.Overwrite).json("C:\\empcl\\data\\json\\test\\extractSessionIndex")

    val SelectedSessionDS = ExtractHourIndex2SessionsDS.groupByKey(_.getString(0)).mapGroups((hour, iter) => {
      val datas = iter.toArray
      val data = datas(0)
      val indexsArr = data.getString(1).split(",")
      val indexsBuffer = new ArrayBuffer[Int]()

      // 用于保存选中的session_id
      val selectedSessionBuffer = new ArrayBuffer[String]

      for (i <- indexsArr.indices) {
        indexsBuffer.append(indexsArr(i).toInt)
      }

      val insertSql_Extract = "insert into session_random_extract values (?,?,?,?,?)"
      for (index <- datas.indices) {
        if (indexsBuffer.contains(index)) {
          val selectedData = datas(index)
          val session_id = selectedData.getString(2)
          val startTime = selectedData.getString(3)
          val search_keywords = selectedData.getString(4)
          val click_category_ids = selectedData.getString(5)
          selectedSessionBuffer.append(session_id)

          // 插入数据库
          JdbcPoolHelper.getJdbcPoolHelper.execute(insertSql_Extract, pstmt => {
            pstmt.setInt(1, task.taskId)
            pstmt.setString(2, session_id)
            pstmt.setString(3, startTime)
            pstmt.setString(4, search_keywords)
            pstmt.setString(5, click_category_ids)

            pstmt.addBatch()

          })
        }
      }

      val selectedSession = selectedSessionBuffer.mkString(",")

      SelectedSession(hour, selectedSession)
    })

    /**
      * root
      * |-- user_id: long (nullable = true) 0
      * |-- session_id: string (nullable = true)
      * |-- action_time: string (nullable = true)
      * |-- search_keyword: string (nullable = true)
      * |-- click_category_id: long (nullable = true)
      * |-- page_id: long (nullable = true) 5
      * |-- click_product_id: long (nullable = true)
      * |-- order_category_ids: string (nullable = true)
      * |-- order_product_ids: string (nullable = true)
      * |-- pay_category_ids: string (nullable = true)
      * |-- pay_product_ids: string (nullable = true)
      * |-- username: string (nullable = true) 11
      * |-- name: string (nullable = true)
      * |-- age: integer (nullable = true)
      * |-- professional: string (nullable = true)
      * |-- city: string (nullable = true) 15
      * |-- sex: string (nullable = true)
      */
    val HourDetailSessionDS = filtered2FullInfoDS.map(row => {
      val action_time = row.getString(2)
      val hour = action_time.split(" ")(1).split(":")(0)
      val user_id = row.getString(0)
      val session_id = row.getString(1)
      val search_keyword = row.getString(3)
      val click_category_id = row.getString(4)
      val page_id = row.getString(5)
      val click_product_id = row.getString(6)
      val order_category_ids = row.getString(7)
      val order_product_ids = row.getString(8)
      val pay_category_ids = row.getString(9)
      val pay_product_ids = row.getString(10)

      HourDetailSession(hour, session_id, user_id, action_time, search_keyword,
        click_category_id, page_id, click_product_id, order_category_ids,
        order_product_ids, pay_category_ids, pay_product_ids)
    })

    /**
      * |-- hour: string (nullable = true)
      * |-- session_ids: string (nullable = true) 1
      * |-- session_id: string (nullable = true) 2
      * |-- user_id: string (nullable = true) 3
      * |-- action_time: string (nullable = true) 4
      * |-- search_keyword: string (nullable = true) 5
      * |-- click_category_id: string (nullable = true) 6
      * |-- page_id: string (nullable = true) 7
      * |-- click_product_id: string (nullable = true) 8
      * |-- order_category_ids: string (nullable = true) 9
      * |-- order_product_ids: string (nullable = true) 10
      * |-- pay_category_ids: string (nullable = true) 11
      * |-- pay_product_ids: string (nullable = true) 12
      */
    val SelectedSessionDetail4HourDS = SelectedSessionDS.join(HourDetailSessionDS, "hour")

    SelectedSessionDetail4HourDS.filter(row => {
      val session_ids = row.getString(1)
      val session_idArr = session_ids.split(",")
      val session_idBuffer = new ArrayBuffer[String]
      for (index <- session_idArr.indices) {
        val selectedSessionIndex = session_idArr(index)
        session_idBuffer.append(selectedSessionIndex)
      }
      val session_id = row.getString(2)
      if (session_idBuffer.contains(session_id)) {
        // 满足条件，插入detail表中
        val user_id = row.getString(3)
        val page_id = row.getString(7)
        val action_time = row.getString(4)
        val search_keyword = row.getString(5)
        val click_category_id = row.getString(6)
        val click_product_id = row.getString(8)
        val order_category_ids = row.getString(9)
        val order_product_ids = row.getString(10)
        val pay_category_ids = row.getString(11)
        val pay_product_ids = row.getString(12)

        val insertSql_detail = "insert into session_detail values (?,?,?,?,?,?,?,?,?,?,?,?)"
        JdbcPoolHelper.getJdbcPoolHelper.execute(insertSql_detail, pstmt => {
          pstmt.setInt(1, task.taskId)
          pstmt.setString(2, user_id)
          pstmt.setString(3, session_id)
          pstmt.setString(4, page_id)
          pstmt.setString(5, action_time)
          pstmt.setString(6, search_keyword)
          pstmt.setString(7, click_category_id)
          pstmt.setString(8, click_product_id)
          pstmt.setString(9, order_category_ids)
          pstmt.setString(10, order_product_ids)
          pstmt.setString(11, pay_category_ids)
          pstmt.setString(12, pay_product_ids)

          pstmt.addBatch()
        })
      }
      true
    })


    sparkSession.stop()
  }

  // 计算session访问时长落于区间的个数
  def calculateVisitLength(visitLength: Short, aggrLengthInfo: String): String = {

    var newAggrLengthInfo = ""

    if (visitLength >= 0 && visitLength <= 3) {
      val count = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_1s_3s).get.toShort
      newAggrLengthInfo = StringUtils.setFieldInConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_1s_3s, (count + 1).toString)
    } else if (visitLength >= 4 && visitLength <= 6) {
      val count = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_4s_6s).get.toShort
      newAggrLengthInfo = StringUtils.setFieldInConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_4s_6s, (count + 1).toString)
    } else if (visitLength >= 7 && visitLength <= 9) {
      val count = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_7s_9s).get.toShort
      newAggrLengthInfo = StringUtils.setFieldInConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_7s_9s, (count + 1).toString)
    } else if (visitLength >= 10 && visitLength <= 30) {
      val count = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_10s_30s).get.toShort
      newAggrLengthInfo = StringUtils.setFieldInConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_10s_30s, (count + 1).toString)
    } else if (visitLength > 30 && visitLength <= 60) {
      val count = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_30s_60s).get.toShort
      newAggrLengthInfo = StringUtils.setFieldInConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_30s_60s, (count + 1).toString)
    } else if (visitLength > 60 && visitLength <= 180) {
      val count = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_1m_3m).get.toShort
      newAggrLengthInfo = StringUtils.setFieldInConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_1m_3m, (count + 1).toString)
    } else if (visitLength > 180 && visitLength <= 600) {
      val count = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_3m_10m).get.toShort
      newAggrLengthInfo = StringUtils.setFieldInConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_3m_10m, (count + 1).toString)
    } else if (visitLength > 600 && visitLength <= 1800) {
      val count = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_10m_30m).get.toShort
      newAggrLengthInfo = StringUtils.setFieldInConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_10m_30m, (count + 1).toString)
    } else if (visitLength > 1800) {
      val count = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_30m).get.toShort
      newAggrLengthInfo = StringUtils.setFieldInConcatString(aggrLengthInfo, "\\|", Constants.TIME_PERIOD_30m, (count + 1).toString)
    }
    if (StringUtils.isNotEmpty(newAggrLengthInfo)) {
      newAggrLengthInfo
    } else {
      aggrLengthInfo
    }
  }

  // 计算session访问步长落于区间的个数
  def calculateStepLength(stepLength: Short, aggrLengthInfo: String): String = {

    var newAggrLengthInfo = ""

    if (stepLength >= 1 && stepLength <= 3) {
      val count = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.STEP_PERIOD_1_3).get.toShort
      newAggrLengthInfo = StringUtils.setFieldInConcatString(aggrLengthInfo, "\\|", Constants.STEP_PERIOD_1_3, (count + 1).toString)
    } else if (stepLength >= 4 && stepLength <= 6) {
      val count = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.STEP_PERIOD_4_6).get.toShort
      newAggrLengthInfo = StringUtils.setFieldInConcatString(aggrLengthInfo, "\\|", Constants.STEP_PERIOD_4_6, (count + 1).toString)
    } else if (stepLength >= 7 && stepLength <= 9) {
      val count = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.STEP_PERIOD_7_9).get.toShort
      newAggrLengthInfo = StringUtils.setFieldInConcatString(aggrLengthInfo, "\\|", Constants.STEP_PERIOD_7_9, (count + 1).toString)
    } else if (stepLength >= 10 && stepLength <= 30) {
      val count = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.STEP_PERIOD_10_30).get.toShort
      newAggrLengthInfo = StringUtils.setFieldInConcatString(aggrLengthInfo, "\\|", Constants.STEP_PERIOD_10_30, (count + 1).toString)
    } else if (stepLength > 30 && stepLength <= 60) {
      val count = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.STEP_PERIOD_30_60).get.toShort
      newAggrLengthInfo = StringUtils.setFieldInConcatString(aggrLengthInfo, "\\|", Constants.STEP_PERIOD_30_60, (count + 1).toString)
    } else if (stepLength > 60) {
      val count = StringUtils.getFieldFromConcatString(aggrLengthInfo, "\\|", Constants.STEP_PERIOD_60).get.toShort
      newAggrLengthInfo = StringUtils.setFieldInConcatString(aggrLengthInfo, "\\|", Constants.STEP_PERIOD_60, (count + 1).toString)
    }

    if (StringUtils.isNotEmpty()) {
      newAggrLengthInfo
    } else {
      aggrLengthInfo
    }
  }

}

case class Task(taskId: Int, taskName: String, createTime: String, startTime: String,
                finishTime: String, taskType: String, taskStatus: String, taskParam: String
               ) extends Serializable

case class SessionPartInfo(session_id: String, user_id: String, action_time: String,
                           search_keyword: String, click_category_id: String
                          )

case class SessionFullInfo(session_id: String, user_id: String, action_time: String,
                           search_keyword: String, click_category_id: String, page_id: String,
                           click_product_id: String, order_category_ids: String, order_product_ids: String,
                           pay_category_ids: String, pay_product_ids: String)

case class UserInfo(user_id: String, username: String, name: String, age: String,
                    professional: String, city: String, sex: String)

case class SessionAggrInfo(sessionId: String, userId: String, visitLength: String, stepLength: String,
                           keyWordsInfos: String, clickCategoryInfos: String)

case class SessionAggrStat(sessionNum: Int, time_s1_3: Int, time_s4_6: Int, time_s7_9: Int,
                           time_s10_30: Int, time_s30_60: Int, time_m1_3: Int, time_m3_10: Int,
                           time_m10_30: Int, time_m30: Int, step_1_3: Int, step_4_6: Int, step_7_9: Int,
                           step_10_30: Int, step_30_60: Int, step_60: Int
                          )

case class SessionHour(session_id: String, time: String, startTime: String)

case class ExtractHourSessionIndex(hour: String, indexs: String)

case class ExtractHourSession(hour: String, session_id: String, startTime: String,
                              search_keywords: String, click_category_ids: String)

case class ExtractRandomSession(session_id: String, startTime: String, search_keywords: String, click_category_ids: String)

case class SelectedSession(hour: String, session_ids: String)

case class HourDetailSession(hour: String, session_id: String, user_id: String, action_time: String,
                             search_keyword: String, click_category_id: String, page_id: String,
                             click_product_id: String, order_category_ids: String, order_product_ids: String,
                             pay_category_ids: String, pay_product_ids: String)